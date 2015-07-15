f4js = require('fuse4js')
Promise = require('bluebird')
redis = Promise.promisifyAll(require('redis'))
# Promise.longStackTraces();

class ENOENTError extends Error
    constructor: (@message="No such file or directory", @errno=2) ->

# Id stands for object id (or inode number)
ROOT_ID = 1
CURR_ID_PREFIX = 'current_id'

# File type enums
TYPE =
    REGULAR: '0'
    DIRECTORY: '1'
    to_str: (type) ->
        return switch type
            when '0' then 'Regular file'
            when '1' then 'Directory'
            else 'Unknown type'
Object.freeze(TYPE)

###
class Obj
    constructor: (@id, @type=TYPE.REGULAR) ->
        @size = 0
    is_reg: ->
        return @type == TYPE.REGULAR
    is_dir: ->
        return @type == TYPE.DIRECTORY

root = new Obj(ROOT_ID, TYPE.DIRECTORY)
###

###
# File's all fields in a Redis hash
object_fields = ['type', 'mode', size]
# delete specific fields for a given id
# https://github.com/NodeRedis/node_redis/issues/369
# return client.send_commandAsync('hdel', [id].concat(object_fields))
###

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
    return client.incrAsync(CURR_ID_PREFIX)

create_at_p = (pid, name, type=TYPE.REGULAR, mode=0o777) ->
    # TODO check pid is existing and directory
    # TODO check name non exist
    # Front end (FUSE, VFS) may check as well
    # create an object and add the directory entry to the parent

    created_id = 0
    return get_next_id_p().then((id) ->
        created_id = id
        return create_by_id_p(id, type, mode)
    ).then((id) ->
        client.hsetAsync("#{pid}_entries", name, id)
    ).then(() ->
            return Promise.resolve(created_id)
    ).tap(() ->
            console.log("Entry #{name} [#{created_id}] added to directory [#{pid}]")
    ).catch((e) ->
        # TODO rollback: delete the created object?
        console.log("Error during create_at(#{pid}, #{name}): ", e)
    )

# TODO validate path?
lookup_p = (path) ->
    chain = Promise.resolve(ROOT_ID)
    return chain if path == '/'

    comps = path.split('/')
    comps[1..].forEach((name) ->
        chain = chain.then((pid) ->
            # console.log("Looking up '#{name}' in directory [#{pid}]")
            return client.hgetAsync("#{pid}_entries", name).then((id) ->
                return Promise.reject(new ENOENTError()) if !id
                console.log("Found '#{name}' [#{id}] in directory [#{pid}]")
                return id
            )
        )
    )
    return chain

# fuse hander
getattr = (path, cb) ->
    console.log("getattr", arguments)
    err = 0
    stat = {}
    lookup_p(path).then((id) ->
        return client.hgetallAsync(id)
    ).then((obj) ->
        console.log(obj)
        if obj['type'] == TYPE.REGULAR
            stat.size = obj.size
            stat.mode = 0o010777
        else
            stat.size = 4096
            stat.mode = 0o040777
    ).catch((e) ->
        err = e.errno ? 99
        console.log("Error during getattr: ", e)
    ).finally(() ->
        cb(-err, stat)
    )

readdir = (path, cb) ->
    console.log("readdir", arguments)
    err = 0
    names = []
    lookup_p(path).then((id) ->
        return client.hgetallAsync("#{id}_entries")
    ).then((obj) ->
        for k, v of obj
            names.push(k)
        console.log(names)
    ).catch((e) ->
        console.log("Error during readdir: ", e)
    ).finally(() ->
        cb(err, names)
    )

open = (path, flags, cb) ->
    console.log("open", arguments)
read = (path, offset, len, buf, fh, cb) ->
    console.log("read", arguments)
write = (path, offset, len, buf, fh, cb) ->
    console.log("read", arguments)
release = (path, fh, cb) ->
    console.log("release", arguments)
create = (path, mode, cb) ->
    console.log("create", arguments)
unlink = (path, cb) ->
    console.log("unlink", arguments)
rename = (src, dst, cb) ->
    console.log("rename", arguments)
mkdir = (path, mode, cb) ->
    console.log("mkdir", arguments)
rmdir = (path, cb) ->
    console.log("rmdir", arguments)
init = (cb) ->
    console.log("init", arguments)
    cb()
setxattr = (path, name, value, size, a, b, c) ->
    console.log("setxattr", arguments)
statfs = (cb) ->
    # console.log("statfs", arguments)
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
        return client.setnxAsync(CURR_ID_PREFIX, 1)
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

