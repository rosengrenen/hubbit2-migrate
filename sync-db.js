require("dotenv/config");

const { Client } = require("pg");
const { v4: uuidv4 } = require("uuid");
const axios = require("axios");

const data = require("./hubbit-data.json");

let cid_user_ids = new Map();

const a_date = new Date("2014-01-01");

(async function () {
	try {
		const client = new Client({
			host: String(process.env.DB_HOST),
			port: parseInt(process.env.DB_PORT, 10),
			db: String(process.env.DB_DATABASE),
			user: String(process.env.DB_USERNAME),
			password: String(process.env.DB_PASSWORD),
		});
		await client.connect();

		// TODO: fetch from gamma
		// Map cid => uuid
		let cids = new Set();
		data.mac_addrs.forEach((mac_addr) => {
			cids.add(mac_addr.cid);
		});
		data.sessions.forEach((session) => {
			cids.add(session.cid);
		});
		data.user_sessions.forEach((user_session) => {
			cids.add(user_session.cid);
		});

		const GAMMA_URL = String(process.env.GAMMA_URL);
		const GAMMA_API_KEY = String(process.env.GAMMA_API_KEY);
		if (GAMMA_API_KEY.length > 0) {
			console.log("Getting user ids from gamma");
			const result = await axios.get(GAMMA_URL + "/api/users/minified", {
				headers: {
					Authorization: "pre-shared " + GAMMA_API_KEY,
				},
			});

			let _cid_user_ids = new Map();
			for (const user of result.data) {
				_cid_user_ids.set(user.cid, user.id);
			}

			cids.forEach((cid) => {
				let uid = _cid_user_ids.get(cid);
				if (uid === undefined) {
					console.log(
						'WARNING: Skipping user with cid "' +
							cid +
							"\" since it doesn't exist in Gamma"
					);
				} else {
					cid_user_ids.set(cid, uid);
				}
			});
		} else {
			console.log("Generating random user ids");
			cids.forEach((cid) => {
				cid_user_ids.set(cid, uuidv4());
			});
		}

		console.log("Removing old data");
		await client.query("DELETE FROM mac_addresses");
		await client.query("DELETE FROM sessions");
		await client.query("DELETE FROM user_sessions");

		// Mac addresses
		await sync_mac_addresses(client);

		// Sessions
		await sync_sessions(client);

		// User sessions
		await sync_user_sessions(client);

		process.exit(0);
	} catch (e) {
		console.error(e);
		process.exit(1);
	}
})();

async function sync_mac_addresses(client) {
	console.log("Syncing MAC addresses");
	const queryText = `
INSERT INTO mac_addresses (user_id, address, device_name)
SELECT data.user_id, data.address, data.device_name
FROM UNNEST($1::uuid[], $2::CHAR(17)[], $3::VARCHAR[]) as data(user_id, address, device_name)
  `;

	let filtered_mac_addrs = data.mac_addrs.filter(
		(mac_addr) => cid_user_ids.get(mac_addr.cid) !== undefined
	);

	let uuids = filtered_mac_addrs.map((mac_addr) =>
		cid_user_ids.get(mac_addr.cid)
	);
	let addresses = filtered_mac_addrs.map((mac_addr) => mac_addr.mac);
	let device_names = filtered_mac_addrs.map(
		(mac_addr) => mac_addr.device_name || ""
	);

	console.log("Checking MAC addresses data");
	console.log(
		"All mac entry ids are ok:",
		uuids.every((x) => x.length === 36)
	);
	console.log(
		"All mac addressess are ok:",
		addresses.every((x) => x.length === 17)
	);
	console.log(
		"All devices names are ok:",
		device_names.every((x) => x !== null && x !== undefined && x.length >= 0)
	);
	await client.query(queryText, [uuids, addresses, device_names]);
	console.log("Successfully synced MAC addresses");
}

async function sync_sessions(client) {
	console.log("Syncing sessions");
	const queryText = `
INSERT INTO sessions (user_id, mac_address, start_time, end_time)
SELECT data.user_id, data.mac_address, data.start_time, data.end_time
FROM UNNEST($1::uuid[], $2::CHAR(17)[], $3::TIMESTAMPTZ[], $4::TIMESTAMPTZ[]) as data(user_id, mac_address, start_time, end_time)
  `;

	let filtered_sessions = data.sessions.filter(
		(session) => cid_user_ids.get(session.cid) !== undefined
	);

	let uuids = filtered_sessions.map((session) => cid_user_ids.get(session.cid));
	let addresses = filtered_sessions.map((session) => session.mac_address);
	let start_times = filtered_sessions.map(
		(session) => new Date(session.start_time)
	);
	let end_times = filtered_sessions.map(
		(session) => new Date(session.end_time)
	);

	console.log("Checking sessions data");
	console.log(
		"All session ids are ok:",
		uuids.every((x) => x.length === 36)
	);
	console.log(
		"All session addresses are ok:",
		addresses.every((x) => x.length === 17)
	);
	console.log(
		"All session start times are ok:",
		start_times.every((x) => x > a_date)
	);
	console.log(
		"All session end times are ok:",
		end_times.every((x) => x > a_date)
	);
	await client.query(queryText, [uuids, addresses, start_times, end_times]);
	console.log("Successfully synced sessions");
}

async function sync_user_sessions(client) {
	console.log("Syncing user sessions");
	const queryText = `
INSERT INTO user_sessions (user_id, start_time, end_time)
SELECT data.user_id, data.start_time, data.end_time
FROM UNNEST($1::uuid[], $2::TIMESTAMPTZ[], $3::TIMESTAMPTZ[]) as data(user_id, start_time, end_time)
  `;

	let filtered_user_sessions = data.user_sessions.filter(
		(user_session) => cid_user_ids.get(user_session.cid) !== undefined
	);

	let uuids = filtered_user_sessions.map((user_session) =>
		cid_user_ids.get(user_session.cid)
	);
	let start_times = filtered_user_sessions.map(
		(user_session) => new Date(user_session.start_time)
	);
	let end_times = filtered_user_sessions.map(
		(user_session) => new Date(user_session.end_time)
	);

	console.log("Checking user sessions data");
	console.log(
		"All user session ids are ok:",
		uuids.every((x) => x.length === 36)
	);
	console.log(
		"All user session start times are ok:",
		start_times.every((x) => x > a_date)
	);
	console.log(
		"All user session end times are ok:",
		end_times.every((x) => x > a_date)
	);
	await client.query(queryText, [uuids, start_times, end_times]);
	console.log("Successfully synced sessions");
}
