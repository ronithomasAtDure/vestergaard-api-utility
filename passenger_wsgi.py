import sys, os

INTERP = "/home/appsduredemos/public_html/vestergaard-utility/env/bin/python"

if sys.executable != INTERP: os.execl(INTERP, INTERP, *sys.argv)

from main import app as application