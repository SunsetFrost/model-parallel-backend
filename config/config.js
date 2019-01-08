const config = {
    port: 2732,
    docker: [
        {
            name: 'parallel.data0',
            ip: '10.36.0.0',
            cpu: '1',
            memory: '2g',
            type: 'data',
            auth: 'master'
        },
        {
            name: 'parallel.calc0',
            ip: '10.36.0.2',
            cpu: '2',
            memory: '2g',
            type: 'calc',
            auth: 'master'
        }
    ]
}

export default config