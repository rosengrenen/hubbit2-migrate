import 'dotenv/config';

import { Client } from 'pg';
import { v4 as uuidv4 } from 'uuid';
import axios from 'axios';
import fs from 'fs';
import { HubbitData } from './types';

const raw_data = fs.readFileSync('hubbit-data.json', { encoding: 'utf8' });
const data: HubbitData = JSON.parse(raw_data);

let cid_user_ids = new Map();

(async function () {
  try {
    const client = new Client({
      host: String(process.env.DB_HOST),
      port: parseInt(String(process.env.DB_PORT), 10),
      database: String(process.env.DB_DATABASE),
      user: String(process.env.DB_USERNAME),
      password: String(process.env.DB_PASSWORD),
    });
    await client.connect();

    // TODO: fetch from gamma
    // Map cid => uuid
    let cids = new Set();
    data.devices.forEach(devices => {
      cids.add(devices.cid);
    });
    data.sessions.forEach(session => {
      cids.add(session.cid);
    });
    data.user_sessions.forEach(user_session => {
      cids.add(user_session.cid);
    });

    const GAMMA_URL = String(process.env.GAMMA_URL);
    const GAMMA_API_KEY = String(process.env.GAMMA_API_KEY);
    if (GAMMA_API_KEY.length > 0) {
      console.log('Getting user ids from gamma');
      const result = await axios.get(GAMMA_URL + '/api/users/minified', {
        headers: {
          Authorization: 'pre-shared ' + GAMMA_API_KEY,
        },
      });

      let _cid_user_ids = new Map();
      for (const user of result.data) {
        _cid_user_ids.set(user.cid, user.id);
      }

      cids.forEach(cid => {
        let uid = _cid_user_ids.get(cid);
        if (uid === undefined) {
          console.log('WARNING: Skipping user with cid "' + cid + '" since it doesn\'t exist in Gamma');
        } else {
          cid_user_ids.set(cid, uid);
        }
      });
    } else {
      console.log('Generating random user ids');
      cids.forEach(cid => {
        cid_user_ids.set(cid, uuidv4());
      });
    }

    console.log('Removing old data');
    await client.query('DELETE FROM devices');
    await client.query('DELETE FROM sessions');
    await client.query('DELETE FROM user_sessions');
    await client.query('DELETE FROM study_years');
    await client.query('DELETE FROM study_periods');

    // Mac addresses
    await sync_mac_addresses(client);

    // Sessions
    await sync_sessions(client);

    // User sessions
    await sync_user_sessions(client);

    // Study years
    await sync_study_years(client);

    // Study periods
    await sync_study_periods(client);

    process.exit(0);
  } catch (e) {
    console.error(e);
    process.exit(1);
  }
})();

async function sync_mac_addresses(client: Client) {
  console.log('Syncing MAC addresses');
  const queryText = `
INSERT INTO devices (user_id, address, name)
SELECT data.user_id, data.address, data.name
FROM UNNEST($1::uuid[], $2::CHAR(17)[], $3::VARCHAR[]) as data(user_id, address, name)
  `;

  let filtered_mac_addrs = data.devices.filter(device => cid_user_ids.get(device.cid) !== undefined);

  let uuids = filtered_mac_addrs.map(device => cid_user_ids.get(device.cid));
  let addresses = filtered_mac_addrs.map(device => device.mac);
  let device_names = filtered_mac_addrs.map(device => device.device_name || '');

  console.log('Checking MAC addresses data');
  console.log(
    'All mac entry ids are ok:',
    uuids.every(x => x.length === 36),
  );
  console.log(
    'All mac addressess are ok:',
    addresses.every(x => x.length === 17),
  );
  console.log(
    'All devices names are ok:',
    device_names.every(x => x !== null && x !== undefined && x.length >= 0),
  );
  await client.query(queryText, [uuids, addresses, device_names]);
  console.log('Successfully synced MAC addresses');
}

async function sync_sessions(client: Client) {
  console.log('Syncing sessions');
  const queryText = `
INSERT INTO sessions (user_id, mac_address, start_time, end_time)
SELECT data.user_id, data.mac_address, data.start_time, data.end_time
FROM UNNEST($1::uuid[], $2::CHAR(17)[], $3::TIMESTAMPTZ[], $4::TIMESTAMPTZ[]) as data(user_id, mac_address, start_time, end_time)
  `;

  let filtered_sessions = data.sessions.filter(session => cid_user_ids.get(session.cid) !== undefined);

  let uuids = filtered_sessions.map(session => cid_user_ids.get(session.cid));
  let addresses = filtered_sessions.map(session => session.mac_address);
  let start_times = filtered_sessions.map(session => session.start_time);
  let end_times = filtered_sessions.map(session => session.end_time);

  console.log('Checking sessions data');
  console.log(
    'All session ids are ok:',
    uuids.every(x => x.length === 36),
  );
  console.log(
    'All session addresses are ok:',
    addresses.every(x => x.length === 17),
  );
  await client.query(queryText, [uuids, addresses, start_times, end_times]);
  console.log('Successfully synced sessions');
}

async function sync_user_sessions(client: Client) {
  console.log('Syncing user sessions');
  const queryText = `
INSERT INTO user_sessions (user_id, start_time, end_time)
SELECT data.user_id, data.start_time, data.end_time
FROM UNNEST($1::uuid[], $2::TIMESTAMPTZ[], $3::TIMESTAMPTZ[]) as data(user_id, start_time, end_time)
  `;

  let filtered_user_sessions = data.user_sessions.filter(
    user_session => cid_user_ids.get(user_session.cid) !== undefined,
  );

  let uuids = filtered_user_sessions.map(user_session => cid_user_ids.get(user_session.cid));
  let start_times = filtered_user_sessions.map(user_session => user_session.start_time);
  let end_times = filtered_user_sessions.map(user_session => user_session.end_time);

  console.log('Checking user sessions data');
  console.log(
    'All user session ids are ok:',
    uuids.every(x => x.length === 36),
  );
  await client.query(queryText, [uuids, start_times, end_times]);
  console.log('Successfully synced sessions');
}

const study_years = [
  { year: 2015, start_date: [2015, 8, 18], end_date: [2016, 8, 15] },
  { year: 2016, start_date: [2016, 8, 16], end_date: [2017, 8, 15] },
  { year: 2017, start_date: [2017, 8, 16], end_date: [2018, 8, 15] },
  { year: 2018, start_date: [2018, 8, 16], end_date: [2019, 8, 15] },
  { year: 2019, start_date: [2019, 8, 16], end_date: [2020, 8, 15] },
  { year: 2020, start_date: [2020, 8, 16], end_date: [2021, 8, 15] },
  { year: 2021, start_date: [2021, 8, 16], end_date: [2022, 8, 15] },
];

async function sync_study_years(client: Client) {
  console.log('Syncing study years');
  const queryText = `
INSERT INTO study_years (year, start_date, end_date)
SELECT data.year, data.start_date, data.end_date
FROM UNNEST($1::INTEGER[], $2::DATE[], $3::DATE[]) as data(year, start_date, end_date)
  `;

  const years = study_years.map(({ year }) => year);
  const start_dates = study_years.map(({ start_date }) => `${start_date[0]}-${start_date[1]}-${start_date[2]}`);
  const end_dates = study_years.map(({ end_date }) => `${end_date[0]}-${end_date[1]}-${end_date[2]}`);

  await client.query(queryText, [years, start_dates, end_dates]);
  console.log('Successfully synced study years');
}

const study_periods = [
  { year: 2015, period: 1, start_date: [2015, 8, 31], end_date: [2015, 11, 1] },
  { year: 2015, period: 2, start_date: [2015, 11, 2], end_date: [2016, 1, 17] },
  { year: 2015, period: 3, start_date: [2016, 1, 18], end_date: [2016, 3, 20] },
  { year: 2015, period: 4, start_date: [2016, 3, 21], end_date: [2016, 6, 4] },
  { year: 2016, period: 0, start_date: [2016, 6, 5], end_date: [2016, 8, 28] },
  { year: 2016, period: 1, start_date: [2016, 8, 29], end_date: [2016, 10, 29] },
  { year: 2016, period: 2, start_date: [2016, 10, 30], end_date: [2017, 1, 14] },
  { year: 2016, period: 3, start_date: [2017, 1, 15], end_date: [2017, 3, 18] },
  { year: 2016, period: 4, start_date: [2017, 3, 19], end_date: [2017, 6, 9] },
  { year: 2017, period: 0, start_date: [2017, 6, 10], end_date: [2017, 8, 27] },
  { year: 2017, period: 1, start_date: [2017, 8, 28], end_date: [2017, 10, 29] },
  { year: 2017, period: 2, start_date: [2017, 10, 30], end_date: [2018, 1, 14] },
  { year: 2017, period: 3, start_date: [2018, 1, 15], end_date: [2018, 3, 18] },
  { year: 2017, period: 4, start_date: [2018, 3, 19], end_date: [2018, 6, 5] },
  { year: 2018, period: 0, start_date: [2018, 6, 6], end_date: [2018, 9, 2] },
  { year: 2018, period: 1, start_date: [2018, 9, 3], end_date: [2018, 11, 4] },
  { year: 2018, period: 2, start_date: [2018, 11, 5], end_date: [2019, 1, 20] },
  { year: 2018, period: 3, start_date: [2019, 1, 21], end_date: [2019, 3, 24] },
  { year: 2018, period: 4, start_date: [2019, 3, 25], end_date: [2019, 6, 8] },
  { year: 2019, period: 0, start_date: [2019, 6, 9], end_date: [2019, 9, 2] },
  { year: 2019, period: 1, start_date: [2019, 9, 3], end_date: [2019, 11, 4] },
  { year: 2019, period: 2, start_date: [2019, 11, 5], end_date: [2020, 1, 20] },
  { year: 2019, period: 3, start_date: [2020, 1, 21], end_date: [2020, 3, 24] },
  { year: 2019, period: 4, start_date: [2020, 3, 25], end_date: [2020, 6, 8] },
  { year: 2020, period: 0, start_date: [2020, 6, 9], end_date: [2020, 8, 30] },
  { year: 2020, period: 1, start_date: [2020, 8, 31], end_date: [2020, 11, 1] },
  { year: 2020, period: 2, start_date: [2020, 11, 2], end_date: [2021, 1, 17] },
  { year: 2020, period: 3, start_date: [2021, 1, 18], end_date: [2021, 3, 21] },
  { year: 2020, period: 4, start_date: [2021, 3, 22], end_date: [2021, 6, 6] },
  { year: 2021, period: 0, start_date: [2021, 6, 7], end_date: [2021, 8, 29] },
  { year: 2021, period: 1, start_date: [2021, 8, 30], end_date: [2021, 10, 31] },
  { year: 2021, period: 2, start_date: [2021, 11, 1], end_date: [2022, 1, 16] },
  { year: 2021, period: 3, start_date: [2022, 1, 17], end_date: [2022, 3, 20] },
  { year: 2021, period: 4, start_date: [2022, 3, 21], end_date: [2022, 6, 5] },
];

async function sync_study_periods(client: Client) {
  console.log('Syncing study periods');
  const queryText = `
INSERT INTO study_periods (year, period, start_date, end_date)
SELECT data.year, data.period, data.start_date, data.end_date
FROM UNNEST($1::INTEGER[], $2::INTEGER[], $3::DATE[], $4::DATE[]) as data(year, period, start_date, end_date)
  `;

  const years = study_periods.map(({ year }) => year);
  const periods = study_periods.map(({ period }) => period);
  const start_dates = study_periods.map(({ start_date }) => `${start_date[0]}-${start_date[1]}-${start_date[2]}`);
  const end_dates = study_periods.map(({ end_date }) => `${end_date[0]}-${end_date[1]}-${end_date[2]}`);

  await client.query(queryText, [years, periods, start_dates, end_dates]);
  console.log('Successfully synced study periods');
}
