# quick notes for myself
Compute shaders go in a compute pipeline, which is seperate from graphics pipeline and much simpler
Workgroups are created via dispatch and are 3 dimensional, invocations within them can also be 3 dimensional.

limits to sizes and counts of these things sohuld be checked in vkPhysiscalDeviceLimits