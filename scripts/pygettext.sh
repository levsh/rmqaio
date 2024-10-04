#!/bin/sh

cd "$(dirname "$0")"

python pygettext.py -d rmqaio -o ../locales/rmqaio.pot ../rmqaio.py
