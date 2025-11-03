# 1.Schema Design Decisions
  I used CATEGORY#{category} as the partition key so that all papers in the same category are grouped together. This enables efficient queries for recent or date-range papers within each category using the sort key date#arxiv_id.  
  #GSIs：3  
  denormalization trade-offs：Papers duplicated across categories, authors, and keywords.  

# 2.Denormalization Analysis 
  Average number of DynamoDB items per paper: 15  
  Storage multiplication factor：15.0x  
  Keyword items caused the most duplication.  

# 3.Query Limitations
  Global aggregate queries are not efficient.  
  Because DynamoDB cannot perform global scans, joins, or aggregations across partitions.  

# 4.When to Use DynamoDB
  I would choose DynamoDB when the workload needs high scalability, predictable low-latency lookups, and well-defined access patterns.  
  DynamoDB trades query flexibility for scalability and speed, while PostgreSQL offers rich querying but requires more management.  
  
# 5.EC2 Deployment
  EC2 instance public IP：i-038672782cd1032a0  
  IAM role ARN:arn:aws:iam::373229397576:user/jzren  
  Challenges:
    Needed to configure proper DynamoDB and EC2 permissions for the IAM user.  
    Resolved credential errors by attaching the correct IAM policy and reconfiguring AWS CLI.  
    



