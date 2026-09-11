select title_id, title_name, launch_date, owner
from {{ ref('titles') }}
