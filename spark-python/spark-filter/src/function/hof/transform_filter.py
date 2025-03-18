# SELECT transform(array(1, 2, 3), x -> x + 1);

column_list = ["load_cd", "transform_cd"]
col_str = '(" + ".join([col({columns}) for columns in ["load_cd", "transform_cd"]])) >= lit(0)'
