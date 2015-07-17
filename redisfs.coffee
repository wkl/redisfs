f4js = require('fuse4js')
Promise = require('bluebird')
redis = Promise.promisifyAll(require('redis'))
# Promise.longStackTraces();
assert = require('assert')
pp = require('path')    # path parser

# Id stands for object id (or inode number)
ROOT_ID = 1
CURR_ID_KEY = 'current_id'
ENTRIES_KEY = (id) -> "#{id}_entries"
INTERNAL_ERR = 99

# File type enums
TYPE =
    UNKNOWN: '-1'
    REGULAR: '0'
    DIRECTORY: '1'
    to_str: (type) ->
        return switch type
            when '0' then 'Regular file'
            when '1' then 'Directory'
            else 'Unknown type'
Object.freeze(TYPE)

is_linux = require('os').platform == 'linux'  # otherwise we assume it is bsd/darwin

class ENOENT extends Error
    constructor: (@message="No such file or directory", @errno=2) ->
class EBADF extends Error
    constructor: (@message="Bad file descriptor", @errno=9) ->
class EEXIST extends Error
    constructor: (@message="File exists", @errno=17) ->
class ENOTDIR extends Error
    constructor: (@message="Not a directory", @errno=20) ->
class ENOTSUP extends Error
    constructor: (@message="Function not implemented", @errno=45) ->
        @errno = 252 if is_linux
class ENOTEMPTY extends Error
    constructor: (@message="Directory not empty", @errno=66) ->
        @errno = 39 if is_linux

# @type is set to unknown if we didn't retrieve it from redis
class Obj
    constructor: (@id, @type=TYPE.UNKNOWN) ->
        @size = -1
        @id = parseInt(@id) # cb(0, @id) in open() requires it is int
    is_reg: ->
        return @type == TYPE.REGULAR
    is_dir: ->
        return @type == TYPE.DIRECTORY
    sync_p: ->
        assert(@content?)
        assert(@size >= 0)
        console.log(@size, @content)
        return client.hmsetAsync(@id, 'size', @size, 'content', @content).tap(() =>
            console.log("#{@size} bytes flushed to redis")
        )
    sanitize: ->
        @size = parseInt(@size)

root = new Obj(ROOT_ID, TYPE.DIRECTORY)

clear_by_id_p = (id) ->
    return client.delAsync(id)

# Create object in redis for given id
# Overwrite existing object
create_by_id_p = (id, type=TYPE.REGULAR, mode=0o777) ->
    return clear_by_id_p(id).then(()->
        client.hmsetAsync(id, 'type', type, 'mode', mode, 'size', 0)
    ).then(() ->
        return Promise.resolve(id)
    ).tap(() ->
        console.log("#{TYPE.to_str(type)} [#{id}] created")
    ).catch((e) ->
        console.log("Error on create_by_id for #{id} :", e)
    )

# Get next available id for new file
# TODO consider race condition
get_next_id_p = () ->
    return client.incrAsync(CURR_ID_KEY)

# Callers make sure pid is valid directory
create_at_p = (pid, name, type=TYPE.REGULAR, mode=0o777) ->
    # create an object and add the directory entry to the parent
    return get_next_id_p().then((id) ->
        return create_by_id_p(id, type, mode)
    ).then((id) ->
        return add_entry_p(pid, name, id)
    ).catch((e) ->
        # TODO rollback: delete the created object?
        console.log("Error during create_at(#{pid}, #{name}): ", e)
    )

lookup_p = (path) ->
    chain = Promise.resolve(ROOT_ID)
    return chain if path == '/'

    comps = path.split('/')
    comps[1..].forEach((name) ->
        chain = chain.then((pid) ->
            # console.log("Looking up '#{name}' in directory [#{pid}]")
            return client.hgetAsync("#{pid}_entries", name).then((id) ->
                return Promise.reject(new ENOENT()) if !id
                console.log("Found '#{name}' [#{id}] in directory [#{pid}]")
                return id
            )
        )
    )
    return chain

lookup_parent_p = (path) ->
    parent_path = pp.dirname(path)
    return lookup_p(parent_path).then((pid) ->
        return get_obj_attr_p(pid, 'type')
    ).then((parent) ->
        if not parent.is_dir()
            return Promise.reject(new ENOTDIR())
        return parent.id
    )

object_fields = ['type', 'mode', 'size']
# delete specific fields for a given id
# https://github.com/NodeRedis/node_redis/issues/369
# return client.send_commandAsync('hdel', [id].concat(object_fields))

# Retrieve fields from redis
get_obj_attr_p = (id, fields...) ->
    if fields.length == 0
        # Get all fields except content
        fields = object_fields

    assert(fields instanceof Array)
    client.hmgetAsync(id, fields...).then((l) ->
        assert(l.length == fields.length)
        obj = new Obj(id)
        obj[k] = l[i] for k, i in fields
        obj.sanitize()
        return obj
    )

# Retrieve directory entries
# return name, id pairs
get_dir_entries_p = (id) ->
    return client.hgetallAsync(ENTRIES_KEY(id))

# Do not override
add_entry_p = (pid, name, id) ->
    return client.hsetnxAsync(ENTRIES_KEY(pid), name, id).then((ret) ->
        if ret == 0
            return Promise.reject(new EEXIST())
        console.log("Entry #{name} [#{id}] added to directory [#{pid}]")
        return id
    )

del_entry_p = (pid, name) ->
    entry_id = 0
    return client.hgetAsync(ENTRIES_KEY(pid), name).then((id) ->
        return id || Promise.reject(now ENOENT())
    ).then((id) ->
        entry_id = id
        client.hlenAsync(ENTRIES_KEY(id))
    ).then((len) ->
        if len > 0
            return Promise.reject(new ENOTEMPTY())
        return client.hdelAsync(ENTRIES_KEY(pid), name)
    ).then((ret) ->
        assert(ret == 1)    # to detect race
        return Promise.resolve(entry_id)
    )

# FUSE hander
getattr = (path, cb) ->
    # console.log("getattr", arguments)
    err = 0
    stat = {}
    lookup_p(path).then((id) ->
        return get_obj_attr_p(id)
    ).then((obj) ->
        console.log("getattr(#{path}):", obj)
        if obj['type'] == TYPE.REGULAR
            stat.size = obj.size
            stat.mode = 0o100777    # TODO translate from obj.mode
        else
            stat.size = 4096
            stat.mode = 0o40777
    ).catch((e) ->
        err = e.errno ? INTERNAL_ERR
        # console.log("Error during getattr: ", e)
    ).finally(() ->
        cb(-err, stat)
    )

readdir = (path, cb) ->
    console.log("readdir", arguments)
    err = 0
    names = []
    lookup_p(path).then((id) ->
        return get_dir_entries_p(id)
    ).then((entries) ->
        names.push(name) for name of entries
        console.log(names)
    ).catch((e) ->
        err = e.errno ? INTERNAL_ERR
        console.log("Error during readdir: ", e)
    ).finally(() ->
        cb(-err, names)
    )

open = (path, flags, cb) ->
    console.log("open", arguments)
    lookup_p(path).then((id) ->
        return get_obj_attr_p(id)
    ).then((obj) ->
        # TODO check flags
        console.log(obj)
        if not obj.is_reg()
            return Promise.reject(new EBADF())
        # return fh so that we save a lookup for subsequent read/write
        cb(0, obj.id)
    ).catch((e) ->
        err = e.errno ? INTERNAL_ERR
        console.log("Error during open: ", e)
        cb(-err)
    )

get_content_p = (id) ->
    return get_obj_attr_p(id, 'content').then((obj) ->
        return obj.content
    )

read = (path, offset, len, buf, fh, cb) ->
    console.log("read", arguments)
    get_obj_attr_p(fh).then((obj) ->    # assume exist
        if not obj.is_reg()
            return Promise.reject(new EBADF())
        return get_content_p(fh)
    ).then((content) ->
        buf.write(content, offset, len, 'utf8')
        cb(buf.length)
    ).catch((e) ->
        err = e.errno ? INTERNAL_ERR
        console.log("Error during write: ", e)
        cb(-err)
    )

# read-modify-update
write = (path, offset, len, buf, fh, cb) ->
    console.log("write", arguments)
    get_obj_attr_p(fh).then((obj) ->    # assume exist
        console.log(obj)
        if not obj.is_reg()
            return Promise.reject(new EBADF())
        obj.content = ''
        if not (offset == 0 and len >= obj.size)    # overwrite all, save a read
            return get_content_p(id).then((content) ->
                obj.content = content
            )
        return obj
    ).then((obj) ->
        assert(obj.size != -1)
        buf_str = buf.toString('utf8')
        if offset == obj.size
            obj.content += buf_str
        else if offset < obj.size
            obj.content = obj.content[..offset-1] + buf_str +
                obj.content[offset+len..]
        else if offset > obj.size
            obj.content += Array(offset - obj.size).join('\0') + buf_str
        obj.size = Math.max(obj.size, offset + len)
        return obj
    ).then((obj) ->
        return obj.sync_p()
    ).then(() ->
        cb(len) # assume full write
    ).catch((e) ->
        err = e.errno ? INTERNAL_ERR
        console.log("Error during write: ", e)
        cb(-err)
    )

release = (path, fh, cb) ->
    console.log("release", arguments)
    cb(0)

create = (path, mode, cb) ->
    console.log("create", arguments)
    err = 0
    name = pp.basename(path)
    lookup_parent_p(path).then((pid) ->
        create_at_p(pid, name, TYPE.REGULAR)
    ).catch((e) ->
        err = e.errno ? INTERNAL_ERR
        console.log(err)
    ).finally(() ->
        cb(-err)
    )

unlink = (path, cb) ->
    console.log("unlink", arguments)
    err = 0
    name = pp.basename(path)
    lookup_parent_p(path).then((pid) ->
        del_entry_p(pid, name)
    ).then((id) ->
        return clear_by_id_p(id)
    ).then((ret) ->
        assert(ret == 1)    # to detect race
    ).catch((e) ->
        err = e.errno ? INTERNAL_ERR
        console.log(err)
    ).finally(() ->
        cb(-err)
    )

rename = (src, dst, cb) ->
    console.log("rename", arguments)
    cb(-(new ENOTSUP()).errno)

# TODO reuse create
mkdir = (path, mode, cb) ->
    console.log("mkdir", arguments)
    err = 0
    name = pp.basename(path)
    lookup_parent_p(path).then((pid) ->
        create_at_p(pid, name, TYPE.DIRECTORY)
    ).catch((e) ->
        err = e.errno ? INTERNAL_ERR
        console.log(err)
    ).finally(() ->
        cb(-err)
    )

rmdir = (path, cb) ->
    console.log("rmdir", arguments)
    unlink(path, cb)

init = (cb) ->
    console.log("init", arguments)
    cb()
setxattr = (path, name, value, size, a, b, c) ->
    console.log("setxattr", arguments)
    cb(0);
statfs = (cb) ->
    # console.log("statfs", arguments)
    # TODO limit file size
    cb(0, {
        bsize: 1000000,
        frsize: 1000000,
        blocks: 1000000,
        bfree: 1000000,
        bavail: 1000000,
        files: 1000000,
        ffree: 1000000,
        favail: 1000000,
        fsid: 1000000,
        flag: 1000000,
        namemax: 1000000
    })
destroy = (cb) ->
    console.log("destory", arguments)
    cb()

# Setup Redis client
client = redis.createClient(6379, '127.0.0.1')

client.on 'error', (err) ->
    console.log(err)

client.on 'connect', () ->
    console.log('Connection to Redis server established')
    main()

# Initialization
init_p = () ->
    # TODO select db using volume id?
    client.selectAsync(0).then(() ->
        # TODO to be removed
        client.flushdbAsync()
    ).then(() ->
        # create current id if not exists
        return client.setnxAsync(CURR_ID_KEY, 1)
    ).catch((e) ->
        console.log('Error during initialization')
        process.exit(1)
    )

main = ->
    init_p().then(() ->
        # create the root '/'
        create_by_id_p(ROOT_ID, TYPE.DIRECTORY)
    ).then(() ->
        # create test files
        Promise.join(
            create_at_p(ROOT_ID, 'foo')
            create_at_p(ROOT_ID, 'dir1', TYPE.DIRECTORY).then((pid) ->
                create_at_p(pid, 'deep')
            )
            create_at_p(ROOT_ID, 'bar')
            create_at_p(ROOT_ID, 'dir2', TYPE.DIRECTORY)
        )
    ).then(() ->
        lookup_p("/dir1/deep").catch((e) ->
            console.log("Error during lookup: ", e)
        )
    ).then(() ->
        get_obj_attr_p(2, 'mode').then((obj) ->
            console.log('get_obj_attr', obj)
        )
    ).then(() ->
        try
            f4js.start("/tmp/mnt", handlers, 0);
        catch e
            console.log("Exception when starting file system: ", e);
    ).catch((e) ->
        console.log("Error during main(): ", e)
    )

handlers =
    getattr: getattr
    readdir: readdir
    open: open
    read: read
    write: write
    release: release
    create: create
    unlink: unlink
    rename: rename
    mkdir: mkdir
    rmdir: rmdir
    init: init
    destroy: destroy
    setxattr: setxattr
    statfs: statfs
    # TODO truncate

