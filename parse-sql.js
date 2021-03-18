const fs = require("fs");
const moment = require("moment-timezone");

const db = fs.readFileSync("./hubbit-backup.sql", { encoding: "utf-8" });

// Mac addresses
const mac_addr_insert_regex = /^INSERT INTO `mac_addresses` VALUES (.*);/gm;
const mac_addr_regex = /^'(.*)','(.*)','.*','.*',('(.*)'|NULL)$/;
const weird_mac_addr_regex = /#<MacAddress:0x00([0-9a-zA-Z]{12})>/;

let mac_addr_insert_statements = db.matchAll(mac_addr_insert_regex);

console.log("Parsing MAC addresses");
let mac_addrs = [];
for (const insert_statement of mac_addr_insert_statements) {
	for (const values of insert_statement[1].slice(1).slice(0, -1).split("),(")) {
		const r = values.match(mac_addr_regex);

		mac_addrs.push({
			mac: r[1],
			cid: r[2],
			device_name: r[3] === "NULL" ? null : r[3].replace(/'/g, ""),
		});
	}
}

// Sessions
const session_insert_regex = /^INSERT INTO `sessions` VALUES (.*);/gm;
const session_regex = /^.*,'(.*)','(.*)','(.*)','(.*)','.*','.*'$/;

let session_insert_statements = db.matchAll(session_insert_regex);

console.log("Parsing sessions");
let sessions = [];
for (const insert_statement of session_insert_statements) {
	for (const values of insert_statement[1].slice(1).slice(0, -1).split("),(")) {
		const r = values.match(session_regex);

		if (r[3].length === 0) {
			continue;
		}

		let mac_address = r[3];
		const r3 = r[3].match(weird_mac_addr_regex);
		if (r3) {
			mac_address =
				r3[1].slice(0, 2).toUpperCase() +
				":" +
				r3[1].slice(2, 4).toUpperCase() +
				":" +
				r3[1].slice(4, 6).toUpperCase() +
				":" +
				r3[1].slice(6, 8).toUpperCase() +
				":" +
				r3[1].slice(8, 10).toUpperCase() +
				":" +
				r3[1].slice(10, 12).toUpperCase();
		}

		sessions.push({
			start_time: moment.tz(r[1], "Europe/Stockholm").toDate(),
			end_time: moment.tz(r[2], "Europe/Stockholm").toDate(),
			mac_address,
			cid: r[4],
		});
	}
}

// User sessions
const user_session_insert_regex = /^INSERT INTO `user_sessions` VALUES (.*);/gm;
const user_session_regex = /^.*,'(.*)','(.*)','(.*)','.*','.*'$/;

let user_session_insert_statements = db.matchAll(user_session_insert_regex);

console.log("Parsing user sessions");
let user_sessions = [];
for (const insert_statement of user_session_insert_statements) {
	for (const values of insert_statement[1].slice(1).slice(0, -1).split("),(")) {
		const r = values.match(user_session_regex);

		user_sessions.push({
			start_time: moment.tz(r[1], "Europe/Stockholm").toDate(),
			end_time: moment.tz(r[2], "Europe/Stockholm").toDate(),
			cid: r[3],
		});
	}
}

fs.writeFileSync(
	"hubbit-data.json",
	JSON.stringify(
		{
			mac_addrs,
			user_sessions,
			sessions,
		},
		null,
		2
	)
);
