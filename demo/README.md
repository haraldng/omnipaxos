# Omnipaxos Demo
This is a small demo of a toy KV store using omnipaxos.

```bash
$ docker-compose up --build
```

### TODO
- [x] abstract socket code into `network` module that provides simple send and receive API
- [x] change from KV example to RocksDB application
- [] add simple dashboard with some user interaction (propose, start scenario?)
- [] implement simple "pre-live-coded" rocksDB application version
