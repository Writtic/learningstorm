#!/bin/bash

CMD="exec bin/storm jar ./topology.jar com.microsoft.example.triggerTestTopology Test "

echo "$CMD"
eval "$CMD"
