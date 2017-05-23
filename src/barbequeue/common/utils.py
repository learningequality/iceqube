import importlib


def stringify_func(func):
    assert callable(func), "function {} passed to stringify_func isn't a function!".format(func)

    fqn = "{module}.{funcname}".format(module=func.__module__, funcname=func.__name__)
    return fqn


def import_stringified_func(funcstring):
    assert isinstance(funcstring, str)

    modulestring, funcname = funcstring.rsplit('.', 1)

    mod = importlib.import_module(modulestring)

    func = getattr(mod, funcname)
    return func
