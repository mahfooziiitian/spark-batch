import os
from configparser import ConfigParser, ExtendedInterpolation

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
CFG_DIR = os.path.join(SCRIPT_DIR, "..", "..", "..", "..", "cfg")

if __name__ == "__main__":
    # --- BasicInterpolation: %(key)s syntax ---
    basic = ConfigParser()
    basic.read(os.path.join(CFG_DIR, "config.cfg"))

    print("=== config.cfg (BasicInterpolation) ===")
    print("a =", basic.get("data", "a"))
    print("b =", basic.get("data", "b"))
    print("c =", basic.get("data", "c"))

    # --- ExtendedInterpolation: ${section:key} syntax ---
    extended = ConfigParser(interpolation=ExtendedInterpolation())
    extended.read(os.path.join(CFG_DIR, "config.conf"))

    print()
    print("=== config.conf (ExtendedInterpolation) ===")
    print("a =", extended.get("data", "a"))
    print("b =", extended.get("data", "b"))
    print("c =", extended.get("data", "c"))
