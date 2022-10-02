financial_year_filter_agriculture = """
SELECT 
    *
FROM
    {view_name}
WHERE
    Industry_name_NZSIOC LIKE '%Agriculture%'    
"""