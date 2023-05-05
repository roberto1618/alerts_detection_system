#!/bin/sh

###############
# Script to launch the Alert Detection System
##############

usage () {
    echo "Script to launch the Alert Detection System."
    echo "The script must be executed every day to send yesterday's alerts."
    echo
    echo "Usage: "
    echo "           ./execute_alerts.sh -e \"[environment]\" [-f \"[future_pred]\"] [-d \"[past_days]\"] [-v \"[ev_pred]\"]"
    echo
    echo "Required arguments: "
    echo "           -e: environment where the alerts will be executed."
    echo "Allowed values: dev, pro"
    echo
    echo "Optional arguments: "
    echo "           -f: True if future predictions for the rest of the current month must be sent."
    echo "Default value: False"
    echo "           -d: Number of past days from where you want to execute the predictions."
    echo "Default value: 1"
    echo "           -v: True if you want to create a table with evaluation metrics of the predictions."
    echo "Default value: False"
}

while getopts "e:f:d:v:" opt; do
    case $opt in
        e)
            ENV=$OPTARG
            ;;
        f)
            FUTURE_PRED=$OPTARG
            ;;
        d)
            PAST_DAYS=$OPTARG
            ;;
        v)
            EV_PRED=$OPTARG
            ;;
        \?)
            usage
            exit 1
            ;;
        :)
            echo "Option -$OPTARG requires an argument." >&2
            exit 1
            ;;
    esac
done

if [ -z "$ENV" ]; then
    usage
    exit 1
fi

if [ -z "$FUTURE_PRED" ]; then
    FUTURE_PRED="False"
fi

if [ -z "$PAST_DAYS" ]; then
    PAST_DAYS=1
fi

if [ -z "$EV_PRED" ]; then
    EV_PRED="False"
fi

python alerts_detection_system.py --env=${ENV} --future-pred=${FUTURE_PRED} --past-days=${PAST_DAYS} --ev-pred=${EV_PRED}
