#!/bin/bash

EXEC='docker exec cortex-apollo'
BASE='/opt/apollo-broker'

#Encrypt password
pass=`$EXEC $BASE/bin/apollo-broker encrypt $2`

#Delete existing/add user
$EXEC sed -i /^$1=/d $BASE/etc/users.properties
$EXEC bash -c "echo \"$1=$pass\" >> $BASE/etc/users.properties"

#Grab existing users
users=`$EXEC grep -F users $BASE/etc/groups.properties`

#Separate statements because the variable expansion isn't working with regex operators
exists1=`$EXEC grep -F "=$1|" $BASE/etc/groups.properties`
exists2=`$EXEC grep -F "|$1|" $BASE/etc/groups.properties`

#Add new user to group if not already there
if [[ -z $exists1 ]] && [[ -z $exists2 ]]; then
    $EXEC sed -i /users/d $BASE/etc/groups.properties && \
    $EXEC bash -c "echo \"$users$1|\" >> $BASE/etc/groups.properties"
fi
