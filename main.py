from utils.args_management import AppArgsManagement
import importlib

if __name__ == "__main__":
    args = AppArgsManagement()
    class_name = args.getClassName()
    package_name = args.getClassPackage()
    conf_path = args.getConfPath()

    module = importlib.import_module(f"{package_name}.{class_name.lower()}")
    cls = getattr(module, class_name)
    etl_instance = cls()
    etl_instance.config_path = conf_path
    etl_instance.run()
