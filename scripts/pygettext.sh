#!/bin/sh

cd "$(dirname "$0")"

python pygettext.py -d rmqaio -o ../rmqaio/locales/rmqaio.pot ../rmqaio
