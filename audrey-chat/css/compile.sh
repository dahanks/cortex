#!/bin/bash

for i in audrey-chat; do
    lessc $i.less > $i.css
done
