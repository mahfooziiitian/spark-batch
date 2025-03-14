from configparser import ConfigParser, ExtendedInterpolation

if __name__ == '__main__':
    config = ConfigParser(interpolation=ExtendedInterpolation())
    config.read('../../../cfg/config.cfg')

    a = config.get("data", "a")
    b = config.get("data", "b")
    c = config.get("data", "c")
    print(a)
    print(b)
    print(f"start c: {c} end")

    config.read('../../cfg/config.conf')

    a = config.get("data", "a")
    b = config.get("data", "b")
    c = config.get("data", "c")
    print(a)
    print(b)
    print(f"start c: {c} end")
