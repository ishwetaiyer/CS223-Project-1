CREATE TABLE simulation_logging (
      ID varchar(255) NOT NULL,
      name varchar(300),
      running_time integer,
      PRIMARY KEY (ID)
);

CREATE TABLE query_logging (
    simulation_id integer references simulation_logging ('ID') ,
    running_time integer
);
