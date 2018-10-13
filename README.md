# Python ElasticSearch tool

Simple tool to index elastic search data. By adding your own `converter` data dynamically then a tool will take care the remaining process.

## HOW TO RUN

```bash
λ python run.py -h
usage: run.py [-h] [-c CONFIG] [-e {local,prod,aws}] -d DATA [-i INDEX]
              [-t TYPE] [-s SCRIPT] [-v]

optional arguments:
  -h, --help            show this help message and exit
  -c CONFIG, --config CONFIG
                        Config file in json
  -e {local,prod,aws}, --env {local,prod,aws}
                        Environment name
  -d DATA, --data DATA  Data json file
  -i INDEX, --index INDEX
                        Elastic search index
  -t TYPE, --type TYPE  Elastic search type
  -s SCRIPT, --script SCRIPT
                        Data Conversion python script file
  -v, --verbose         Verbose log
```

Before execute `run.py`:

- Adapt your config file as follow [config.json](./config.json)
- Customize your `converter` as follow [converter.py](./converter.py)

## TODO

- [ ] Multi thread and async request
- [ ] CLI
- [ ] Test coverage
- [ ] Python 2 compatible
