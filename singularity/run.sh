#!/usr/bin/env bash

# The following command will open a bash shell in the container:

singularity run -B $LUMIA_DATA:/data -B $LUMIA_SCRATCH:/scratch -B $LUMIA_TMP:/tmp -B $LUMIA_OUTPUT:/output lumia