SELECT map_zip_with(map(1, 10, 2, 20), map(1, 1, 2, 2), (k, v1, v2) -> v1 + v2);
