import RocksDB from "rocksdb";
import levelup from "levelup";
import {columnFamiliesNames, ZbColumnFamilies} from "./zbColumnFamilies";
import { Buffer } from 'node:buffer'
import {unpack} from "msgpackr";
import memoizee from 'memoizee';


const CACHE_TIMEOUT = 300000  // 5 minutes

/**
 * @param cfName The column family name as a fully typed-out string, e.g. "PROCESS_CACHE_BY_ID_AND_VERSION".
 *               See zbColumnFamilies.ts for the awaited values
 * @param offset Either 0 or 1. Don't use other numbers :)
 */
const columnFamilyNametoInt64Bytes = (cfName: keyof typeof ZbColumnFamilies, offset: number) => {
  const cfNum = ZbColumnFamilies[cfName]
  return int64ToBytes(cfNum + offset)
}

const int64ToBytes = (i : number) : Uint8Array => {
  const buf = Buffer.alloc(8);
  buf.writeBigUInt64BE(BigInt(i))
  return buf
}

export class ZDB extends levelup {
  constructor(path: string) {
    super(
      RocksDB(path),
      {
        createIfMissing: false,
        readOnly: true,
        infoLogLevel: 'error'
      }
    )
  }

  async refresh() {
    // force refresh of the db
    if (this.isOpen()) await this.close()
    await this.open()
  }

  async walkColumnFamily(columnFamilyName: keyof typeof ZbColumnFamilies, callback: (key: string, value: Buffer) => void): Promise<void> {
    return new Promise(async (resolve, reject) => {
      this.createReadStream({
        gte: columnFamilyNametoInt64Bytes(columnFamilyName, 0),
        lt: columnFamilyNametoInt64Bytes(columnFamilyName, 1),
      })
        .on('data', function (data) {
          callback(data.key, data.value)
        })
        .on('error', function (err) {
          console.error(`Error when walking on ColumnFamily ${columnFamilyName}`, err)
          reject(err)
        })
        .on('close', function () {
        })
        .on('end', function () {
          resolve()
        })
    })
  }

  /*
   * Get a Map of column families as (columnFamilyName, count)
   * Get only the ones with at least a value
   */
  async ColumnFamiliesCount() {
    await this.refresh()

    try {
      const columFamiliesCounted = new Map<string, number>()

      for (let columnFamilyName of columnFamiliesNames) {
        let count: number | undefined;
        await this.walkColumnFamily(columnFamilyName, function () {
          !count ? count = 1 : count++;
        })

        count && columFamiliesCounted.set(columnFamilyName, count)
      }

      return columFamiliesCounted
    } catch (e) {
      console.error(e)
      // get all or nothing if an error raised
      return new Map<string, number>()
    }
  }

  MemoizedColumnFamiliesCount = memoizee(this.ColumnFamiliesCount, { maxAge: CACHE_TIMEOUT, promise: true })

  async getIncidentsMessageCount() {
    await this.refresh()

    try {
      const incidentMessages: string[] = []

      await this.walkColumnFamily('INCIDENTS', function(key, value) {
        const unpackedValue = unpack(value)
        incidentMessages.push(unpackedValue?.incidentRecord?.errorMessage)
      })

      const incidentCountPerMessage = new Map<string, number>()

      // group by errorMessage and set the mesure
      incidentMessages.forEach((message) => {
        // add to Map
        if (!incidentCountPerMessage.has(message)) incidentCountPerMessage.set(message, 0)
        incidentCountPerMessage.set(message, incidentCountPerMessage.get(message)! +1)
      })

      return incidentCountPerMessage

    } catch (e) {
      console.error(e)
      // get all or nothing if an error raised
      return new Map<string, number>()
    }
  }

  MemoizedGetIncidentsMessageCount = memoizee(this.getIncidentsMessageCount, { maxAge: CACHE_TIMEOUT, promise: true })
}
