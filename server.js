const app = require('express')()
const http = require('http').Server(app)
const io = require('socket.io')(http)
const port = process.env.PORT || 3000

const context = process.env.CONTEXT || 'hcd'

console.info('context: ' + context)

http.listen(port)

const Redis = require('ioredis')
const redis = new Redis(6379, 'localhost')
const pub = new Redis(6379, 'localhost')


const messageQuorum = (presentes) => {
    let q = presentes.length

    let ret = {
        quorum: q,
        presentes: presentes
    }

    switch (context) {
        case 'hcd':
            ret.hayQuorum = q > 7
            ret.ausentes = 14 - q
            break
        case 'fimper':
            ret.hayQuorum = q >= 14
            ret.ausentes = 26 - q
            break
        default:
            throw 'El contexto ' + context + ' no está implementado'
    }

    console.info('messageQuorum', ret)

    return ret
}


redis.subscribe('message', function(err, count) {})

function quorum() {
    const q = pub.hgetall('presentes').then((presentes) => {
        presentes = Object.keys(presentes).map(p => parseInt(p))

        io.emit('message', {
            type: 'quorum',
            data: messageQuorum(presentes)
        })
    })
}

setInterval(quorum, 5000);

io.on('connection', function(socket) {
    console.log(socket.handshake.query)
    const concejalId = socket.handshake.query.concejalId
    console.log('EVENT connection', concejalId)

    socket.on('disconnect', function() {
        console.log('EVENT disconnect', concejalId)
        if (concejalId) {
            pub.hdel('presentes', concejalId)
            quorum()
        }
    });
    if (concejalId) {
        pub.hset('presentes', concejalId, 1)
        quorum()
    }
});

redis.on('message', function(channel, message) {
    message = JSON.parse(message);

    if (message.deferred) {
        setTimeout(function () {
            console.log('message', message)
            io.emit('message', message)
        }, message.deferred * 1000)
    } else {
        if (message.type === 'votacion.abierta') {
            if (message.data.duracion) {
                let tiempo = 0
                let interval = setInterval(function () {
                    tiempo++

                    message.data.tiempo = tiempo

                    if (tiempo >= message.data.duracion) {
                        clearInterval(interval)
                    } else {
                        io.emit('message', {
                            type: 'votacion.tick',
                            data: message.data
                        })
                    }
                }, 1000)
            }
        }

        console.log('message', message)
        io.emit('message', message)
    }
});
