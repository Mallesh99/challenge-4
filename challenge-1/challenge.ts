/**
 * The entry point function. This will download the given dump file, extract/decompress it,
 * parse the CSVs within, and add the data to a SQLite database.
 * This is the core function you'll need to edit, though you're encouraged to make helper
 * functions!
 */
import * as fs from "fs";
import * as path from "path";
import * as https from "https";
import * as zlib from "zlib";
import * as tar from "tar";
import knex from "knex";
import { DUMP_DOWNLOAD_URL } from "./resources";
import { SQLITE_DB_PATH } from "./resources";
import { parse } from "csv-parse";

export async function processDataDump() {
  /**
   * Put your code here!
   */

  const downloadUrl = DUMP_DOWNLOAD_URL;
  const tmpDirectory = path.join("../challenge-1", "tmp");
  const outDirectory = path.join("../challenge-1", "out");

  fs.mkdirSync(tmpDirectory, { recursive: true });
  fs.mkdirSync(outDirectory, { recursive: true });

  // Creating a Knex instance for SQLite
  const db = knex({
    client: "sqlite3",
    connection: {
      filename: SQLITE_DB_PATH,
    },
    useNullAsDefault: true,
  });

  //Defined the schema and created tables using Knex
  //organizations table
  db.schema
    .createTable("organizations", (table) => {
      table.string("Index").notNullable();
      table.string("Organization Id").primary();
      table.string("Name").notNullable();
      table.string("Website").notNullable();
      table.string("Country").notNullable();
      table.string("Description").notNullable();
      table.string("Founded").notNullable();
      table.string("Industry").notNullable();
      table.string("Number of employees").notNullable();
    })
    .then(() => {
      console.log('Table "organizations" created.');
    });

  //customers table
  db.schema
    .createTable("customers", (table) => {
      table.string("Index").notNullable();
      table.string("Customer Id").primary();
      table.string("First Name").notNullable();
      table.string("Last Name").notNullable();
      table.string("Company");
      table.string("City").notNullable();
      table.string("Country").notNullable();
      table.string("Phone 1");
      table.string("Phone 2");
      table.string("Email").notNullable();
      table.string("Subscription Date").notNullable();
      table.string("Website").notNullable();
    })
    .then(() => {
      console.log('Table "customers" created.');
    });

  // Downloading and extracting the tar.gz file
  function dwndandExt() {
    return new Promise<void>((resolve, reject) => {
      const tarballPath = path.join(tmpDirectory, "dump.tar.gz");
      const extractPath = path.join(tmpDirectory, "dump.tar");

      const fileStream = fs.createWriteStream(tarballPath);
      https
        .get(downloadUrl, (response) => {
          response.pipe(fileStream);
          fileStream.on("finish", () => {
            fileStream.close();
            // Extract the tarball
            fs.mkdirSync(extractPath);
            fs.createReadStream(tarballPath)
              .pipe(zlib.createGunzip())
              .pipe(tar.extract({ cwd: extractPath }))
              .on("end", () => {
                resolve();
              });
          });
        })
        .on("error", (err) => {
          fs.unlink(tarballPath, () => {});
          reject(err);
        });
    });
  }

  // Starting the download and processing pipeline
  await dwndandExt();
  const csvFilePath = path.resolve("./tmp/dump.tar/dump/customers.csv");
  const headers = [
    "Index",
    "Customer Id",
    "First Name",
    "Last Name",
    "Company",
    "City",
    "Country",
    "Phone 1",
    "Phone 2",
    "Email",
    "Subscription Date",
    "Website",
  ];

  //parsing the customers csv file
  const fileStream = fs.createReadStream(csvFilePath);
  const csvParser = parse({
    delimiter: ",",
    columns: headers,
  });

  let buffer: any[] = [];
  let rowCount = 0;
  let flag = 0;

  fileStream.pipe(csvParser);

  //adding data to customer table here in 100 batches each
  csvParser.on("data", (row) => {
    //console.log(buffer);
    if (flag != 0) {
      buffer.push(row);
      rowCount++;
    }
    if (flag == 0) {
      flag = 1;
    }

    if (rowCount >= 100) {
      db.batchInsert("customers", buffer, buffer.length);
      buffer = [];
      rowCount = 0;
    }
  });

  csvParser.on("end", () => {
    if (buffer.length > 0) {
      db.batchInsert("customers", buffer, buffer.length);
      buffer = [];
    }
  });

  csvParser.on("error", (error) => {
    console.error(error);
  });

  //organizations table

  const csvFilePath_org = path.resolve("./tmp/dump.tar/dump/organizations.csv");
  const headers_org = [
    "Index",
    "Organization Id",
    "Name",
    "Website",
    "Country",
    "Description",
    "Founded",
    "Industry",
    "Number of employees",
  ];

  //parsing the organizations csv file
  const fileStream_org = fs.createReadStream(csvFilePath_org);
  const csvParser_org = parse({
    delimiter: ",",
    columns: headers_org,
  });

  let buffer_org: any[] = [];
  let rowCount_org = 0;
  let flag_org = 0;

  fileStream_org.pipe(csvParser_org);

  //adding data to organizations table here in 100 batches each
  csvParser_org.on("data", (row) => {
    //console.log(buffer);
    if (flag_org != 0) {
      buffer_org.push(row);
      rowCount_org++;
    }
    if (flag_org == 0) {
      flag_org = 1;
    }

    if (rowCount_org >= 100) {
      db.batchInsert("organizations", buffer_org, buffer_org.length);
      buffer_org = [];
      rowCount_org = 0;
    }
  });

  csvParser_org.on("end", () => {
    if (buffer_org.length > 0) {
      db.batchInsert("organizations", buffer_org, buffer_org.length);
      buffer_org = [];
    }
  });

  csvParser_org.on("error", (error) => {
    console.error(error);
  });
}