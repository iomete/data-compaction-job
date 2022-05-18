# iomete: Spark Job Template

- https://setuptools.pypa.io/en/latest/userguide/declarative_config.html?highlight=options.extras_require#configuring-setup-using-setup-cfg-files

## Prepare the dev environment

```shell
virtualenv .env #or python3 -m venv .env
source .env/bin/activate

pip install -e ."[dev]"
```

## Run test

```shell
pytest
```
