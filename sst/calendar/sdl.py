"""SST model description. argv contains the validated image, output and clock."""

import sys

import sst

image, events, period = sys.argv[1:]
fabric = sst.Component("calendar", "wsecalendar.fabric")
fabric.addParams({"image": image, "events": events, "clock": period + "ps"})
