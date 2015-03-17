#!/bin/sh
pwd=`pwd`
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Node.py A; clear\"" > /dev/null
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Node.py B; clear\"" > /dev/null
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Node.py C; clear\"" > /dev/null
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Node.py D; clear\"" > /dev/null
osascript -e "tell application \"Terminal\" to do script \"cd $pwd; python Coordinator.py E; clear\"" > /dev/null
