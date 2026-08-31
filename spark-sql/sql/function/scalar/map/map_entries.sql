SELECT map(1, 'a', 2, 'b');

SELECT map_entries(map(1, 'a', 2, 'b'));

SELECT explode(map_entries(map(1, 'a', 2, 'b')));
