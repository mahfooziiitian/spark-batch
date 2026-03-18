from pyspark.sql.window import Window
from pyspark.sql import WindowSpec, functions as F

class WindowDefiner:
    """Define and use window specifications"""
    
    @staticmethod
    def define_ordered_window(partition_cols, order_cols, 
                              order_direction="asc", 
                              window_frame=None) -> WindowSpec:
        """Define an ordered window specification"""
        spec = Window.partitionBy(*partition_cols)
        
        # Add ordering
        for i, order_col in enumerate(order_cols):
            if isinstance(order_direction, list):
                direction = order_direction[i] if i < len(order_direction) else "asc"
            else:
                direction = order_direction
            
            if direction.lower() == "desc":
                spec = spec.orderBy(F.col(order_col).desc())
            else:
                spec = spec.orderBy(F.col(order_col).asc())
        
        # Add window frame if specified
        if window_frame:
            frame_start, frame_end = window_frame
            spec = spec.rowsBetween(frame_start, frame_end)
        
        return spec
    
    @staticmethod
    def define_unbounded_window(partition_cols, order_cols)-> WindowSpec:
        """Define window with unbounded preceding/following"""
        return Window.partitionBy(*partition_cols) \
                     .orderBy(*order_cols) \
                     .rowsBetween(Window.unboundedPreceding, 
                                 Window.unboundedFollowing)
    
    @staticmethod
    def define_moving_window(partition_cols, order_cols, 
                            preceding=1, following=0)-> WindowSpec:
        """Define moving window with specific preceding/following"""
        return Window.partitionBy(*partition_cols) \
                     .orderBy(*order_cols) \
                     .rowsBetween(-preceding, following)

