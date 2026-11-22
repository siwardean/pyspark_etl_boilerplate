"""Run any job: python main.py --job package.module.ClassName --conf path/to/config.yaml"""
import argparse
import importlib


def parse_args(argv=None):
    parser = argparse.ArgumentParser(description="Run a job")
    parser.add_argument("--job", required=True, help="e.g. jobs.examples.my_batch_job.MyBatchJob")
    parser.add_argument("--conf", required=True, help="path to the YAML config file")
    parser.add_argument(
        "--param", action="append", default=[], metavar="KEY=VALUE",
        help="run parameter, repeatable (e.g. --param run_date=2016-03-15)",
    )
    args = parser.parse_args(argv)
    try:
        args.params = dict(item.split("=", 1) for item in args.param)
    except ValueError:
        parser.error("--param expects KEY=VALUE")
    return args


def load_job_class(path):
    module_name, _, class_name = path.rpartition(".")
    return getattr(importlib.import_module(module_name), class_name)


def main(argv=None):
    args = parse_args(argv)
    load_job_class(args.job)(args.conf).run(**args.params)


if __name__ == "__main__":
    main()
