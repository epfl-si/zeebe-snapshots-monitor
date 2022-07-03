import {initDBReader, walkColumnFamily, zdb} from "./zeebeDB";
import {columnFamiliesNames} from "./zbColumnFamilies";
import {runtimeDir} from "./folders";

const client = require("prom-client");

client.collectDefaultMetrics({
  app: 'zeebe-db-monitor',
  prefix: '',
  timeout: 10000,
  gcDurationBuckets: [0.001, 0.01, 0.1, 1, 2, 5],
});

// Create a histogram of the time to read the DB
export const zeebe_db_read_duration_seconds = new client.Histogram({
    name: 'zeebe_db_read_duration_seconds',
    help: 'Duration to count the entries for all the columnFamilies in ZeebeDB in seconds',
})

new client.Gauge({
  name: `zeebe_db_column_family_entries`,
  help: `Number of elements per column families inside the db`,
  labelNames: ['db_name', 'column_family'],
  async collect() {
    // Set the mesure on all the column family
    if (!zdb) await initDBReader(runtimeDir)

    columnFamiliesNames.map(async (columnFamilyName) => {
      if (columnFamilyName !== 'INCIDENTS') {  // exclude INCIDENT, as there is a specific gauge for it
        let count: number | undefined;
        await walkColumnFamily(zdb!, columnFamilyName, function () {
          !count ? count = 1 : count++;
        })
        if (count) {
          this.set(
            {db_name: 'runtime', column_family: columnFamilyName}, count
          );
        }
      }
    })
  }
})

new client.Gauge({
  name: `zeebe_db_column_family_incident_entries`,
  help: `Number of incidents per errorMessage inside the db`,
  labelNames: ['db_name', 'error_message'],
  async collect() {
    // Set the mesure on all the column family
    if (!zdb) await initDBReader(runtimeDir)

    const incidentMessages: string[] = []

    await walkColumnFamily(zdb!, 'INCIDENTS', function(key, value) {
      incidentMessages.push(value)
    })

    const incidentCountPerMessage = new Map<string, number>()

    // group by errorMessage and set the mesure
    incidentMessages.forEach((value: object|string) => {
      value = String(value)  // set to string, for whatever we have

      let start = value.indexOf('errorMessage');
      if (start === -1) return
      start += 'errorMessage'.length

      // totally arbitral value
      start += 2
      const end = start + 35
      const cleanedErrorMessage = value?.substring(start, end)

      // add to Map
      if (!incidentCountPerMessage.has(cleanedErrorMessage)) incidentCountPerMessage.set(cleanedErrorMessage, 0)
      incidentCountPerMessage.set(cleanedErrorMessage, incidentCountPerMessage.get(cleanedErrorMessage)! +1)
    })

    //set the measure
    incidentCountPerMessage.forEach((count, message) => {
      this.set(
        {db_name: 'runtime', error_message: message}, count
      );
    })
  }
})
