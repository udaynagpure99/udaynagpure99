from equalexperts_dataeng_exercise.scripts.exercise import *

create_view = """CREATE OR REPLACE VIEW blog_analysis.outlier_weeks 
        AS (
        SELECT extract('year' From creationDate) as Year 
                         ,week(creationDate )  as WeekNumber                         
                         ,CAST(ABS(1 - (SUM(VoteTypeId) / AVG(VoteTypeId)))AS INT) VoteCount
        FROM blog_analysis.votes
        GROUP BY
        extract('year' From creationDate)  
        ,week(creationDate)  
        HAVING ABS(1 - (SUM(VoteTypeId) / AVG(VoteTypeId))) > 0.2 
                
        )
    """
get_data = "SELECT * FROM blog_analysis.outlier_weeks"       
run_query(query=create_view)
run_query(query=get_data)
print("View Created Successfully\n")