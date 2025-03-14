# window_frame

Specifies which row to start the window on and where to end it.

## Syntax:

    { RANGE | ROWS } { frame_start | BETWEEN frame_start AND frame_end }

`frame_start` and `frame_end` have the following syntax:

## Syntax:

    UNBOUNDED PRECEDING | offset PRECEDING | CURRENT ROW | offset FOLLOWING | UNBOUNDED FOLLOWING

`offset`: specifies the offset from the position of the current row.

Note: If `frame_end` is omitted it defaults to `CURRENT ROW`.