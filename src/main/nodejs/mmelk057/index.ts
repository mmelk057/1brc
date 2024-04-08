/*
 *  Copyright 2023 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

import { FileHandle, open } from "node:fs/promises";
import { createReadStream } from "node:fs";
import { cpus } from "node:os";
import { Worker, isMainThread, workerData, parentPort } from "node:worker_threads";

/**
 * intermediary data type used to process rows
 * @typedef {Object} Result
 * @property {number} min - minimum 
 * @property {number} max - maximum
 * @property {number} total - total sum
 * @property {number} count - total count
 */
interface Result {
  min: number;
  max: number;
  total: number;
  count: number;
}

/**
 * reference -> [https://1brc.dev/]
 * > input ranges are as follows:
 * >> station name: non-null UTF-8 string of min length 1 character and max lengthof 100 bytes
 * >> temparature value: non-null double in the range [-99.9, 99.9], always with one fractional digit
 */
const STATION_NAME_BYTE_SIZE_MAX = 100;
/**
 * UTF-8 characters range from 1-4 bytes in size (encompassing >=1 code points) 
 * > the largest possible temperature value, w.r.t raw byte size, is '-99.9', involving =5 UTF-8 characters,
 *    each of which involves a single UTF-8 code point
*/
const STATION_VALUE_BYTE_SIZE_MAX = 5;
const ROW_LEN_MAX = STATION_NAME_BYTE_SIZE_MAX + STATION_VALUE_BYTE_SIZE_MAX;

const ROW_DELIM = ';'.charCodeAt(0);
// each row is delimited by an LF character; we can confidently disregard carriage returns
const LINE_DELIM = '\n'.charCodeAt(0);

const MINUS_SIGN = '-'.charCodeAt(0);
const FRACTION_DELIM = '.'.charCodeAt(0);
const ZERO = '0'.charCodeAt(0);
const ONE = '1'.charCodeAt(0);
const TWO = '2'.charCodeAt(0);
const THREE = '3'.charCodeAt(0);
const FOUR = '4'.charCodeAt(0);
const FIVE = '5'.charCodeAt(0);
const SIX = '6'.charCodeAt(0);
const SEVEN = '7'.charCodeAt(0);
const EIGHT = '8'.charCodeAt(0);
const NINE = '9'.charCodeAt(0);

if (isMainThread) {
  const FILE_PATH_ARG = process.argv[2];
  /**
   * main thread handles:
   * > spawning worker threads
   * >> merging processed rows from worker threads
   * >>> rendering output 
  */
  await main(FILE_PATH_ARG);
} else {
  /**
   * worker threads handle:
   * > reading dataset's I/O stream
   * >> processing rows
   * >>> relaying processed rows to main thread
   */
  processStream(workerData.path, workerData.start, workerData.end);
}

/**
 * Processing entrypoint for main thread
 * @param {string} path - file path of dataset to process 
 * @returns {Promise<void>}
 */
async function main(path: string): Promise<void> { 
  const partitions = await partition(path);
  const workers = new Array<Promise<void>>();
  const results = new Map<string, Result>();

  for (let i = 0; i < partitions.length - 1; i++) {
    // spawn worker threads to process independent dataset partitions
    const worker = new Worker(new URL(import.meta.url), {
      workerData: {
        path: path,
        start: partitions[i],
        end: partitions[i + 1],
      }
    }).on('message', (rows: Map<string, Result>) => {
      rows.forEach((row, key) => upsertMergedRow(key, row, results));
    }).on('error', (err) => {
      throw new Error(`Failed to process partition. Error: '${err.message}'`);
    });
    // promisification
    workers.push(new Promise((resolve, reject) => {
      worker.on('exit', resolve);
      worker.on('error', reject);
    }));
  }
  // wait until all workers have completed processing
  await Promise.all(workers);
  renderRows(results);
}

/**
 * Uniformely partition a given uncompressed, unencrypted dataset, such that 
 * data is evenly spread across all available cores
 * @param {string} path - file path of dataset to partition 
 * @returns {Array<number>} - mutually independent set of offsets/positions
 * that represent where to begin processing a given dataset
 */
async function partition(path: string): Promise<Array<number>> {
  if (!path) throw new Error('Empty path argument provided');

  let handle: FileHandle;
  try {
    handle = await open(path, 'r');
    const stats = await handle.stat();
    const numCpus = cpus().length;
    // uniformely partition the dataset, to be distributed across cores
    const partitionSize = Math.floor(stats.size / numCpus);
    /**
     *  ______________
     *  |  |  |  ... |
     *  0        <FILE_SIZE>
     */
    const partitions = [ 0 ];
    const buffer = Buffer.allocUnsafe(ROW_LEN_MAX);
    /**
     * inherently, there's non-uniformity in the distribution of line breaks between rows,
     * so at each partition-sized breakpoint, we're attempting to locate the closest next line break
     * that will correspond to our "true" partition offset, where processing may begin
    */
    for (let partitionOffset = partitionSize; (partitionOffset + ROW_LEN_MAX) < stats.size; partitionOffset += partitionSize) {
      /**
       * we can be confident that, in the worst case scenario, it will take at most 'N' bytes
       * to locate the closest next line break, where 'N' is the maximum row entry size
      */
      await handle.read(buffer, 0, ROW_LEN_MAX, partitionOffset);
      const rowOffset = buffer.indexOf(LINE_DELIM);
      if (rowOffset < 0) throw new Error(`Failed to partition the following file: '${path}'. Unable to locate line break for current chunk: '${buffer.toString()}'`);
      partitions.push(partitionOffset + (rowOffset + 1));
    }
    partitions.push(stats.size);
    return partitions;
  } catch (err) {
    throw new Error(`Failed to partition the following file: '${path}'. Error: '${(<Error>err).message}'`);
  } finally {
    //@ts-ignore
    if (handle) await handle.close();
  }
}

/**
 * Processes a subset of a given dataset
 * @param {string} path - file path of dataset to process 
 * @param {number} start - inclusive beginning of dataset partition to process 
 * @param {number} end - exclusive ending of dataset partition to process
 */
function processStream(path: string, start: number, end: number): void {
  /**
   * processing a readable stream chunk-by-chunk produces non-uniformity
   * w.r.t line breaks on the ending of chunks. The solution here is to store
   * partial row segments, from former chunks, to concatenate with the remaining
   * segment of the row found in the current chunk 
   */
  const remainderBuffer = Buffer.allocUnsafe(ROW_LEN_MAX);
  let remainderBufferCursor = 0;
 
  let stationName: string | null, 
      stationValue: number,
      isStationValueFractional: boolean,
      isStationValueNegative: boolean;

  /**
   * alternating flag indicating whether we're reading the station name (row key)
   * or the temperature value from a given chunk 
  */
  let readingStationName = true;

  const rowInit = () => {
    stationName = null;
    stationValue = 0;
    isStationValueFractional = false;
    isStationValueNegative = false;
  }

  rowInit();

  const results = new Map<string, Result>();

  createReadStream(path, { start, end })
    .on('end', () => parentPort?.postMessage(results))
    .on('error', (err) => { throw new Error(`Failed to process byte range [${start}, ${end}) for file '${path}'. Error: '${err}'`); })
    .on('data', (chunk: Buffer) => {
      // specifies the position where a new row begins (typically, after a line break)
      let rowStartCursor = 0;
      // specifies the position of either a line break ('\n') or an intra-row delimiter (';')
      let breakpointCursor = 0;
      
      for (let chunkCursor = 0; chunkCursor < chunk.length; chunkCursor++) {
        switch (chunk[chunkCursor]) {
          case ROW_DELIM:
            readingStationName = false;
            breakpointCursor = chunkCursor + 1;
            
            // case: we don't have a partially buffered row segment from the previous chunk
            if (remainderBufferCursor === 0) {
              stationName = chunk.toString('utf8', rowStartCursor, chunkCursor);
            } else {
              // edge case: we have a partially buffered row segment from the previous chunk
              stationName = Buffer.concat([
                remainderBuffer.subarray(0, remainderBufferCursor),
                chunk.subarray(rowStartCursor, chunkCursor)
              ]).toString('utf8');
              // equivalent to 'clearing' our buffer
              remainderBufferCursor = 0;
            }
            break;
          case LINE_DELIM:
            readingStationName = true;
            stationValue = isStationValueNegative ? -stationValue : stationValue; 
            // process the former row, after a line break is encountered
            upsertProcessedRow(stationName!, stationValue, results);
            // adjust state to begin reading the input for the next row
            rowStartCursor = chunkCursor + 1;
            breakpointCursor = rowStartCursor;
            rowInit();
            break;
          case MINUS_SIGN:
            if (!readingStationName) isStationValueNegative = true;
            break;
          case FRACTION_DELIM:
            if (!readingStationName) isStationValueFractional = true;
            break;
          case ZERO:
          case ONE:
          case TWO:
          case THREE:
          case FOUR:
          case FIVE:
          case SIX:
          case SEVEN:
          case EIGHT:
          case NINE:
            // SET station value
            if (!readingStationName) {
              /**
               * digits are represented in UTF-8 with increasing code point values
               * i.e. 0=39, 1=40, 2=41, ...
               * we're attempting to normalize digit values to their integer equivalents
               */
              const digit = chunk[chunkCursor] - ZERO;
              /**
               * simple, fast, float-parsing logic.
               * the input we expect is predictably formatted (i.e. 'always a single fractional digit'), so
               * we optimize for speed over flexibility
               */
              if (isStationValueFractional!) stationValue! += digit / 10;
              else stationValue! += (stationValue! * 10) + digit;
            }
            break;
        }
      }
      /**
       * only cache a partial row segment if we're in the midst of reading a station
       * name
       */
      if (readingStationName) {
        remainderBufferCursor = chunk.length - breakpointCursor;
        if (remainderBufferCursor > 0) chunk.copy(remainderBuffer, 0, breakpointCursor, chunk.length);
      }
    });
}

/**
 * Process a row, and perform an upsert within a map that stores intermediary data representations, 
 * summarizing row values (temperatures) for a given row key (station name)
 * @param {string} key - row key (station name)
 * @param {number} value - row value (temperature value)
 * @param {Map<string, Result>} map - map that stores intermediary data representations, summarizing row values for a given row key
 */
function upsertProcessedRow(key: string, value: number, map: Map<string, Result>): void {
  map.set(key, processRow(value, map.get(key)));
}

/**
 * Process a row by updating its intermediary data representation with 
 * an additional, raw data point
 * @param {number} value - raw data point (temperature value) for a given row key (station name)
 * @param {Result} accumulator - intermediary data representation, summarizing row values for a given row key
 * @returns {Result} - updated intermediary data representation
 */
function processRow(value: number, accumulator?: Result): Result {
  if (!accumulator) return { min: value, max: value, total: value, count: 1 };
  accumulator.min = accumulator.min > value ? value : accumulator.min;
  accumulator.max = accumulator.max < value ? value : accumulator.max;
  accumulator.total = Math.round((accumulator.total + value) * 10) / 10;
  accumulator.count++;
  return accumulator;
}

/**
 * Merge two intermediary data representations of a row with the same row key (station name), and perform an upsert
 * within a map that stores intermediary data representations, summarizing row values (temperatures) for a given row key (station name)
 * @param {string} key - row key (station name)
 * @param {Result} row - intermediary representation of a row
 * @param {Map<string, Result>} map - map that stores intermediary data representations, summarizing row values for a given row key
 */
function upsertMergedRow(key: string, row: Result, map: Map<string, Result>): void {
  map.set(key, mergeRows(row, map.get(key)));
}

/**
 * Merge two intermediary data representations of a row with the same row key (station name)
 * @param {Result} rowA - intermediary representation of a row
 * @param {Result} rowB - intermediary representation of a row 
 * @returns {Result} - merged intermediary representation of a row
 */
function mergeRows(rowA?: Result, rowB?: Result): Result {
  if (!rowA && !rowB) throw new Error('Row merging failed. Both rows are empty');
  else if (!rowA) return rowB!;
  else if (!rowB) return rowA!;
  return {
    min: rowA.min < rowB.min ? rowA.min : rowB.min,
    max: rowA.max > rowB.max ? rowA.max : rowB.max,
    total: Math.round((rowA.total + rowB.total) * 10) / 10,
    count: rowA.count + rowB.count,
  }
}

/**
 * Render all rows to STDOUT according to the format specification outlined in -> [https://1brc.dev/]
 * > "{ <STATION_NAME>:<MIN>/<MEAN>/<MAX>, ...}"
 * @param {Map<string, Result>} map - map that stores intermediary data representations, summarizing row values for a given row key
 */
function renderRows(map: Map<string, Result>): void {
  process.stdout.write('{');
  Array.from(map)
    .sort((a, b) => a[0].localeCompare(b[0]))
    .forEach(([key, row], index) => {
      // Calculate mean
      const mean = Math.round((row.total / row.count) * 10) / 10;
      process.stdout.write(`${key}=${row.min}/${mean}/${row.max}${index < map.size - 1 ? ', ' : ''}`);
    });
  process.stdout.write('}');
}