export interface HubbitData {
  devices: Device[];
  user_sessions: UserSession[];
  sessions: Session[];
}

export interface Device {
  mac: string;
  cid: string;
  device_name: string | null;
}

export interface UserSession {
  start_time: string;
  end_time: string;
  cid: string;
}

export interface Session {
  start_time: string;
  end_time: string;
  mac_address: string;
  cid: string;
}
