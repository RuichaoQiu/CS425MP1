#!/bin/sh
pwd=`pwd`
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Server.py A; clear\"" > /dev/null
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Server.py B; clear\"" > /dev/null
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Server.py C; clear\"" > /dev/null
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Server.py D; clear\"" > /dev/null
