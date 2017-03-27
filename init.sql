create table vehicle_locations (
  id integer primary key,
  vehicleId string,
  lon string,
  routeTag string,
  predictable boolean,
  speedKmHr integer,
  heading integer,
  lat string,
  secsSinceReport integer
);

create index vehicleId_index on vehicle_locations (vehicleId);
