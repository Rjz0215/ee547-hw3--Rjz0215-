1.Schema Decisions: Natural vs surrogate keys? Why?
  Use surrogate keys for lines, stops, and line_stops.
  Because surrogate keys are fast and flexible.

2.Constraints: What CHECK/UNIQUE constraints did you add?
  Vehicle type: only 'bus' or 'rail' are allowed.
  Geographic range: latitude must be between -90 and 90, and longitude must be between -180 and 180.
  Passenger count: the number of passengers getting on and off must be non-negative.
  Uniqueness constraint: line names and stop names must not be duplicated.
  Sequence uniqueness: within the same line, each stop’s sequence number must be unique.

3.Complex Query: Which query was hardest? Why?
  "Q9: Trips with 3+ delayed stops" is the hardest one.
  Because it involves comparing scheduled and actual times, grouping records by trip, and identifying trips that had delays at multiple stops.

4.Foreign Keys: Give example of invalid data they prevent
  Example: A trip referencing a non-existent line
  
5.When Relational: Why is SQL good for this domain?
  Because SQL can handle structured, interconnected information efficiently.