# Redis Health Check Sidecar 

## About
A simple application/deamon to assist Loadbalancer etc. to check the health of a Redis 
instance.

This is a simple Daemon that listens on a configured port (eg. 8000) for 
a *HTTP GET* request to */master* and either returns a *HTTP 200 OK* if 
the following Redis commands succeed:
1. *AUTH <configured-password>*
2. *PING* and receiving *PONG*
3. *INFO replication* and receiving *role:master*

otherwise return a HTTP 501

## License
Copyright 2026 Southern Cross Solutions (Pty) Ltd

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.

