// Import the connect function from Cloudflare's sockets API
import { connect } from 'cloudflare:sockets';

// --- Global Configuration Variables ---
// These variables hold various settings and states for the worker.
// Many are initialized with default values and can be overridden by environment variables.

let userID = ''; // User ID for authentication, can be set via env.UUID, env.uuid, env.PASSWORD, env.pswd
let proxyIP = ''; // Default proxy IP, can be overridden by env.PROXYIP or env.proxyip
let subConverter = atob('U1VCQVBJLkNNTGl1c3Nzcy5uZXQ='); // Base64 decoded subscription converter URL
let subConfig = atob('aHR0cHM6Ly9yYXcuZ2l0aHVidXNlcmNvbnRlbnQuY29tL0FDTDRTU1IvQUNMNFNTUi9tYXN0ZXIvQ2xhc2gvY29uZmlnL0FDTDRTU1JfT25saW5lX01pbmlfTXVsdGlNb2RlLmluaQ=='); // Base64 decoded subscription configuration URL
let subProtocol = 'https'; // Protocol for the subscription converter
let subEmoji = 'true'; // Flag to enable/disable emojis in subscription names
let socks5Address = ''; // SOCKS5 proxy address, can be overridden by env.HTTP or env.SOCKS5
let parsedSocks5Address = {}; // Parsed SOCKS5 address components (hostname, port, username, password)
let enableSocks = false; // Flag to indicate if SOCKS5 proxy is enabled
let enableHttp = false; // Flag to indicate if HTTP proxy is enabled (for SOCKS5)
let noTLS = 'false'; // Flag to disable TLS, can be overridden by URL search param 'notls'
const expire = 4102329600; // Expiration timestamp (2099-12-31)
let proxyIPs; // Array of proxy IPs
let socks5s; // Array of SOCKS5 addresses
let go2Socks5s = [ // Domains that should use SOCKS5 proxy
	'*ttvnw.net',
	'*tapecontent.net',
	'*cloudatacdn.com',
	'*.loadshare.org',
];
let addresses = []; // Array of preferred addresses (TLS enabled)
let addressesapi = []; // Array of preferred API addresses (TLS enabled)
let addressesnotls = []; // Array of preferred addresses (TLS disabled)
let addressesnotlsapi = []; // Array of preferred API addresses (TLS disabled)
let addressescsv = []; // Array of CSV addresses for IP testing
let DLS = 8; // Download speed limit for CSV addresses
let remarkIndex = 1; // CSV remark column offset
let FileName = atob('ZWRnZXR1bm5lbA=='); // Base64 decoded default filename for subscriptions
let BotToken; // Telegram Bot Token for sending messages
let ChatID; // Telegram Chat ID for sending messages
let proxyhosts = []; // Array of proxy hosts for workers.dev domains
let proxyhostsURL = ''; // URL to fetch proxy hosts from
let RproxyIP = 'false'; // Flag to enable random proxy IP selection
const httpPorts = ["8080", "8880", "2052", "2082", "2086", "2095"]; // Common HTTP ports
let httpsPorts = ["2053", "2083", "2087", "2096", "8443"]; // Common HTTPS ports
let 有效时间 = 7; // Dynamic UUID validity period in days
let 更新时间 = 3; // Dynamic UUID update time in hours (Beijing time)
let userIDLow; // Lowercased user ID (for dynamic UUID)
let userIDTime = ""; // Timestamp for dynamic UUID
let proxyIPPool = []; // Pool of proxy IPs for specific addresses
let path = '/?ed=2560'; // Default path for Vless configuration
let 动态UUID; // Dynamic UUID generated based on env.KEY/TOKEN/userID
let link = []; // Array of additional links
let banHosts = [atob('c3BlZWQuY2xvdWRmbGFyZS5jb20=')]; // Blacklisted hosts
let SCV = 'true'; // Flag to skip TLS certificate verification
let allowInsecure = '&allowInsecure=1'; // Parameter for allowing insecure connections in Vless config

// WebSocket ready states
const WS_READY_STATE_OPEN = 1;
const WS_READY_STATE_CLOSING = 2;

// Pre-computed hexadecimal representations for bytes (0-255)
const byteToHex = Array.from({ length: 256 }, (_, i) => (i + 256).toString(16).slice(1));

// Base64 decoded string for Vless protocol type
const VLESS_PROTOCOL_TYPE_BASE64 = atob('ZG14bGMzTT0=');

// Decoded message for the subscription page footer
const SUBSCRIPTION_FOOTER_MESSAGE = decodeURIComponent(atob('dGVsZWdyYW0lMjAlRTQlQkElQTQlRTYlQjUlODElRTclQkUlQTQlMjAlRTYlOEElODAlRTYlOUMlQUYlRTUlQTQlQTclRTQlQkQlQUMlN0UlRTUlOUMlQTglRTclQkElQkYlRTUlOEYlOTElRTclODklOEMhJTNDYnIlM0UKJTNDYSUyMGhyZWYlM0QlMjdodHRwcyUzQSUyRiUyRnQubWUlMkZDTUxpdXNzc3MlMjclM0VodHRwcyUzQSUyRiUyRnQubWUlMkZDTUxpdXNzc3MlM0MlMkZhJTNFJTNDYnJzJTNFCl0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0lM0NiciUzRQpnaXRodWIlMjAlRTklQjElQjklRTclOUIlQUUlRTUlOUMlQjAlRTUlOUQlODAlMjBTdGFyISVTdGFyISVTdGFyISEhJTNDYnIlM0UKJTNDYSUyMGhyZWYlM0QlMjdodHRwcyUzQSUyRiUyRmdpdGh1Yi5jb20lMkZjbWxpdSUyRmVkZ2V0dW5uZWwlMjclM0VodHRwcyUzQSUyRiUyRmdpdGh1Yi5jb20lMkZjbWxpdSUyRmVkZ2V0dW5uZWwlM0MlMkZhJTNFJTNDYnIlM0ULC0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0tLS0lM0NiciUzRQolMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjMlMjM='));

// Parameters that indicate a subscription request
const subParams = ['sub', 'base64', 'b64', 'clash', 'singbox', 'sb', 'loon'];

/**
 * Main fetch handler for the Cloudflare Worker.
 * This function processes incoming requests, handles Vless over WebSocket connections,
 * serves subscription configurations, and manages KV storage.
 * @param {Request} request The incoming request object.
 * @param {Env} env The environment variables.
 * @param {ExecutionContext} ctx The execution context.
 * @returns {Promise<Response>} The response to the request.
 */
export default {
	async fetch(request, env, ctx) {
		try {
			// Get User-Agent header and convert to lowercase for case-insensitive comparison
			const userAgent = (request.headers.get('User-Agent') || 'null').toLowerCase();
			const url = new URL(request.url);
			const pathName = url.pathname.toLowerCase();

			// --- Initialize User ID and Dynamic UUID ---
			// Prioritize env variables, then fall back to default
			userID = env.UUID || env.uuid || env.PASSWORD || env.pswd || userID;

			// If a dynamic UUID key is provided, generate dynamic UUIDs
			if (env.KEY || env.TOKEN || (userID && !isValidUUID(userID))) {
				动态UUID = env.KEY || env.TOKEN || userID;
				有效时间 = Number(env.TIME) || 有效时间;
				更新时间 = Number(env.UPTIME) || 更新时间;
				const [currentUserID, lowerUserID, timeStr] = await 生成动态UUID(动态UUID);
				userID = currentUserID;
				userIDLow = lowerUserID;
				userIDTime = timeStr;
			}

			// If userID is still not set, return an error response
			if (!userID) {
				return new Response('请设置你的UUID变量，或尝试重试部署，检查变量是否生效？', {
					status: 404,
					headers: { "Content-Type": "text/plain;charset=utf-8" }
				});
			}

			// Generate fake user ID and hostname for obfuscation
			const currentDate = new Date();
			currentDate.setHours(0, 0, 0, 0);
			const timestamp = Math.ceil(currentDate.getTime() / 1000);
			const fakeUserIDMD5 = await 双重哈希(`${userID}${timestamp}`);
			const fakeUserID = [
				fakeUserIDMD5.slice(0, 8),
				fakeUserIDMD5.slice(8, 12),
				fakeUserIDMD5.slice(12, 16),
				fakeUserIDMD5.slice(16, 20),
				fakeUserIDMD5.slice(20)
			].join('-');
			const fakeHostName = `${fakeUserIDMD5.slice(6, 9)}.${fakeUserIDMD5.slice(13, 19)}`;

			// --- Initialize Proxy IPs and SOCKS5 Addresses ---
			proxyIP = env.PROXYIP || env.proxyip || proxyIP;
			proxyIPs = await 整理(proxyIP);
			proxyIP = proxyIPs[Math.floor(Math.random() * proxyIPs.length)]; // Select a random proxy IP

			socks5Address = env.HTTP || env.SOCKS5 || socks5Address;
			socks5s = await 整理(socks5Address);
			socks5Address = socks5s[Math.floor(Math.random() * socks5s.length)]; // Select a random SOCKS5 address

			// Determine if HTTP proxy is enabled for SOCKS5
			enableHttp = env.HTTP ? true : socks5Address.toLowerCase().includes('http://');
			socks5Address = socks5Address.split('//')[1] || socks5Address; // Remove protocol prefix

			// Override go2Socks5s and httpsPorts if environment variables are set
			if (env.GO2SOCKS5) go2Socks5s = await 整理(env.GO2SOCKS5);
			if (env.CFPORTS) httpsPorts = await 整理(env.CFPORTS);
			if (env.BAN) banHosts = await 整理(env.BAN);

			// Parse SOCKS5 address if provided
			if (socks5Address) {
				try {
					parsedSocks5Address = socks5AddressParser(socks5Address);
					RproxyIP = env.RPROXYIP || 'false'; // Set RproxyIP based on env or default
					enableSocks = true;
				} catch (err) {
					console.error('SOCKS5 address parsing error:', err.toString());
					RproxyIP = env.RPROXYIP || (!proxyIP ? 'true' : 'false'); // Fallback RproxyIP
					enableSocks = false;
				}
			} else {
				RproxyIP = env.RPROXYIP || (!proxyIP ? 'true' : 'false'); // Fallback RproxyIP
			}

			// --- Handle WebSocket Upgrade Requests ---
			const upgradeHeader = request.headers.get('Upgrade');
			if (upgradeHeader === 'websocket') {
				// Extract socks5Address from URL search params or path for WebSocket connections
				socks5Address = url.searchParams.get('socks5') || socks5Address;
				if (new RegExp('/socks5=', 'i').test(pathName)) socks5Address = pathName.split('5=')[1];
				else if (new RegExp('/socks://', 'i').test(pathName) || new RegExp('/socks5://', 'i').test(pathName) || new RegExp('/http://', 'i').test(pathName)) {
					enableHttp = pathName.includes('http://');
					socks5Address = pathName.split('://')[1].split('#')[0];
					if (socks5Address.includes('@')) {
						let userPassword = socks5Address.split('@')[0].replaceAll('%3D', '=');
						const base64Regex = /^(?:[A-Z0-9+/]{4})*(?:[A-Z0-9+/]{2}==|[A-Z0-9+/]{3}=)?$/i;
						if (base64Regex.test(userPassword) && !userPassword.includes(':')) userPassword = atob(userPassword);
						socks5Address = `${userPassword}@${socks5Address.split('@')[1]}`;
					}
					go2Socks5s = ['all in']; // Force SOCKS5 for all traffic
				}

				// Re-parse SOCKS5 address for WebSocket if it was updated
				if (socks5Address) {
					try {
						parsedSocks5Address = socks5AddressParser(socks5Address);
						enableSocks = true;
					} catch (err) {
						console.error('SOCKS5 address parsing error (WebSocket):', err.toString());
						enableSocks = false;
					}
				} else {
					enableSocks = false;
				}

				// Extract proxyIP from URL search params or path for WebSocket connections
				if (url.searchParams.has('proxyip')) {
					proxyIP = url.searchParams.get('proxyip');
					enableSocks = false;
				} else if (new RegExp('/proxyip=', 'i').test(pathName)) {
					proxyIP = pathName.toLowerCase().split('/proxyip=')[1];
					enableSocks = false;
				} else if (new RegExp('/proxyip.', 'i').test(pathName)) {
					proxyIP = `proxyip.${pathName.toLowerCase().split("/proxyip.")[1]}`;
					enableSocks = false;
				} else if (new RegExp('/pyip=', 'i').test(pathName)) {
					proxyIP = pathName.toLowerCase().split('/pyip=')[1];
					enableSocks = false;
				}

				return await 维列斯OverWSHandler(request);
			}

			// --- Handle HTTP Requests (Non-WebSocket) ---
			// Load various address lists from environment variables
			if (env.ADD) addresses = await 整理(env.ADD);
			if (env.ADDAPI) addressesapi = await 整理(env.ADDAPI);
			if (env.ADDNOTLS) addressesnotls = await 整理(env.ADDNOTLS);
			if (env.ADDNOTLSAPI) addressesnotlsapi = await 整理(env.ADDNOTLSAPI);
			if (env.ADDCSV) addressescsv = await 整理(env.ADDCSV);
			DLS = Number(env.DLS) || DLS;
			remarkIndex = Number(env.CSVREMARK) || remarkIndex;
			BotToken = env.TGTOKEN || BotToken;
			ChatID = env.TGID || ChatID;
			FileName = env.SUBNAME || FileName;
			subEmoji = env.SUBEMOJI || env.EMOJI || subEmoji;
			if (subEmoji === '0') subEmoji = 'false';
			if (env.LINK) link = await 整理(env.LINK);

			let sub = env.SUB || '';
			subConverter = env.SUBAPI || subConverter;
			if (subConverter.includes("http://")) {
				subConverter = subConverter.split("//")[1];
				subProtocol = 'http';
			} else {
				subConverter = subConverter.split("//")[1] || subConverter;
			}
			subConfig = env.SUBCONFIG || subConfig;

			// Override 'sub' and 'notls' from URL search parameters
			if (url.searchParams.has('sub') && url.searchParams.get('sub') !== '') sub = url.searchParams.get('sub').toLowerCase();
			if (url.searchParams.has('notls')) noTLS = 'true';

			// Set path based on URL search parameters for proxyIP or socks5
			if (url.searchParams.has('proxyip')) {
				path = `/proxyip=${url.searchParams.get('proxyip')}`;
				RproxyIP = 'false';
			} else if (url.searchParams.has('socks5')) {
				path = `/?socks5=${url.searchParams.get('socks5')}`;
				RproxyIP = 'false';
			} else if (url.searchParams.has('socks')) {
				path = `/?socks5=${url.searchParams.get('socks')}`;
				RproxyIP = 'false';
			}

			SCV = env.SCV || SCV;
			if (!SCV || SCV === '0' || SCV === 'false') allowInsecure = '';
			else SCV = 'true';

			// --- Route based on URL Pathname ---
			if (pathName === '/') {
				if (env.URL302) return Response.redirect(env.URL302, 302);
				else if (env.URL) return await 代理URL(env.URL, url);
				else return new Response(JSON.stringify(request.cf, null, 4), {
					status: 200,
					headers: { 'content-type': 'application/json' }
				});
			} else if (pathName === `/${fakeUserID}`) {
				// Serve fake configuration for obfuscation
				const fakeConfig = await 生成配置信息(userID, request.headers.get('Host'), sub, 'CF-Workers-SUB', RproxyIP, url, fakeUserID, fakeHostName, env);
				return new Response(`${fakeConfig}`, { status: 200 });
			} else if (pathName === `/${动态UUID}/edit` || pathName === `/${userID}/edit`) {
				// Serve KV editing page
				const html = await KV(request, env);
				return html;
			} else if (pathName === `/${动态UUID}` || pathName === `/${userID}`) {
				// Serve actual subscription configuration
				await sendMessage(`#获取订阅 ${FileName}`, request.headers.get('CF-Connecting-IP'), `UA: ${userAgent}\n域名: ${url.hostname}\n<tg-spoiler>入口: ${url.pathname + url.search}</tg-spoiler>`);
				const vlessConfig = await 生成配置信息(userID, request.headers.get('Host'), sub, userAgent, RproxyIP, url, fakeUserID, fakeHostName, env);

				const now = Date.now();
				const today = new Date(now);
				today.setHours(0, 0, 0, 0);
				const UD = Math.floor(((now - today.getTime()) / 86400000) * 24 * 1099511627776 / 2);
				let pagesSum = UD;
				let workersSum = UD;
				let total = 24 * 1099511627776;

				// Respond differently based on User-Agent (browser vs. client)
				if (userAgent.includes('mozilla')) {
					return new Response(vlessConfig, {
						status: 200,
						headers: {
							"Content-Type": "text/html;charset=utf-8",
							"Profile-Update-Interval": "6",
							"Subscription-Userinfo": `upload=${pagesSum}; download=${workersSum}; total=${total}; expire=${expire}`,
							"Cache-Control": "no-store",
						}
					});
				} else {
					return new Response(vlessConfig, {
						status: 200,
						headers: {
							"Content-Disposition": `attachment; filename=${FileName}; filename*=utf-8''${encodeURIComponent(FileName)}`,
							"Profile-Update-Interval": "6",
							"Subscription-Userinfo": `upload=${pagesSum}; download=${workersSum}; total=${total}; expire=${expire}`,
						}
					});
				}
			} else {
				// Default fallback for unknown paths
				if (env.URL302) return Response.redirect(env.URL302, 302);
				else if (env.URL) return await 代理URL(env.URL, url);
				else return new Response('不用怀疑！你UUID就是错的！！！', { status: 404 });
			}
		} catch (err) {
			console.error('Fetch handler error:', err);
			return new Response(`Error: ${err.message || err.toString()}`, { status: 500 });
		}
	},
};

/**
 * Handles Vless over WebSocket connections.
 * This function sets up a WebSocket connection and pipes data between the client WebSocket
 * and a remote TCP/SOCKS5/HTTP proxy server.
 * @param {Request} request The incoming WebSocket upgrade request.
 * @returns {Promise<Response>} A WebSocket upgrade response.
 */
async function 维列斯OverWSHandler(request) {
	// Create a new WebSocketPair for the connection
	const webSocketPair = new WebSocketPair();
	const [client, webSocket] = Object.values(webSocketPair);

	// Accept the WebSocket connection
	webSocket.accept();

	let address = '';
	let portWithRandomLog = '';

	// Logger function for connection information
	const log = (/** @type {string} */ info, /** @type {string | undefined} */ event) => {
		console.log(`[${address}:${portWithRandomLog}] ${info}`, event || '');
	};

	// Get early data header from 'sec-websocket-protocol'
	const earlyDataHeader = request.headers.get('sec-websocket-protocol') || '';

	// Create a readable stream from the WebSocket
	const readableWebSocketStream = makeReadableWebSocketStream(webSocket, earlyDataHeader, log);

	// Wrapper for the remote Socket to allow late assignment
	let remoteSocketWapper = { value: null };
	let isDns = false; // Flag to indicate if it's a DNS query

	// Pipe the WebSocket data stream to a WritableStream
	readableWebSocketStream.pipeTo(new WritableStream({
		async write(chunk) {
			if (isDns) {
				// If it's a DNS query, handle it
				return await handleDNSQuery(chunk, webSocket, null, log);
			}
			if (remoteSocketWapper.value) {
				// If remote Socket exists, write data directly
				const writer = remoteSocketWapper.value.writable.getWriter();
				await writer.write(chunk);
				writer.releaseLock();
				return;
			}

			// Process Vless protocol header for the initial connection
			const {
				hasError,
				message,
				addressType,
				portRemote = 443,
				addressRemote = '',
				rawDataIndex,
				维列斯Version = new Uint8Array([0, 0]),
				isUDP,
			} = process维列斯Header(chunk, userID);

			// Set address and port for logging
			address = addressRemote;
			portWithRandomLog = `${portRemote}--${Math.random()} ${isUDP ? 'udp ' : 'tcp '} `;

			if (hasError) {
				throw new Error(message); // Throw error if header processing failed
			}

			// UDP proxy is only enabled for DNS (port 53)
			if (isUDP) {
				if (portRemote === 53) {
					isDns = true;
				} else {
					throw new Error('UDP 代理仅对 DNS（53 端口）启用');
				}
			}

			// Build Vless response header (version and command status)
			const 维列斯ResponseHeader = new Uint8Array([维列斯Version[0], 0]);
			// Get actual client data after the Vless header
			const rawClientData = chunk.slice(rawDataIndex);

			if (isDns) {
				return handleDNSQuery(rawClientData, webSocket, 维列斯ResponseHeader, log);
			}

			// Handle TCP outbound connection
			if (!banHosts.includes(addressRemote)) {
				log(`处理 TCP 出站连接 ${addressRemote}:${portRemote}`);
				await handleTCPOutBound(remoteSocketWapper, addressType, addressRemote, portRemote, rawClientData, webSocket, 维列斯ResponseHeader, log);
			} else {
				throw new Error(`黑名单关闭 TCP 出站连接 ${addressRemote}:${portRemote}`);
			}
		},
		close() {
			log(`readableWebSocketStream 已关闭`);
		},
		abort(reason) {
			log(`readableWebSocketStream 已中止`, JSON.stringify(reason));
		},
	})).catch((err) => {
		log('readableWebSocketStream 管道错误', err);
		safeCloseWebSocket(webSocket); // Ensure WebSocket is closed on pipe error
	});

	// Return a WebSocket upgrade response
	return new Response(null, {
		status: 101,
		// @ts-ignore
		webSocket: client,
	});
}

/**
 * Handles TCP outbound connections, including SOCKS5 and HTTP proxy options.
 * @param {{value: import('cloudflare:sockets').Socket | null}} remoteSocketWapper Wrapper for the remote socket.
 * @param {number} addressType Type of the remote address (IPv4, Domain, IPv6).
 * @param {string} addressRemote Remote address.
 * @param {number} portRemote Remote port.
 * @param {Uint8Array} rawClientData Initial client data to send.
 * @param {WebSocket} webSocket The client WebSocket.
 * @param {Uint8Array} 维列斯ResponseHeader Vless response header.
 * @param {(string)=> void} log Logging function.
 */
async function handleTCPOutBound(remoteSocketWapper, addressType, addressRemote, portRemote, rawClientData, webSocket, 维列斯ResponseHeader, log) {
	/**
	 * Checks if SOCKS5 proxy should be used for a given address based on go2Socks5s patterns.
	 * @param {string} address The target address.
	 * @returns {Promise<boolean>} True if SOCKS5 should be used, false otherwise.
	 */
	async function useSocks5Pattern(address) {
		if (go2Socks5s.includes(atob('YWxsIGlu')) || go2Socks5s.includes(atob('Kg=='))) return true;
		return go2Socks5s.some(pattern => {
			const regexPattern = pattern.replace(/\*/g, '.*');
			const regex = new RegExp(`^${regexPattern}$`, 'i');
			return regex.test(address);
		});
	}

	/**
	 * Connects to the target address/port, optionally via SOCKS5 or HTTP proxy, and writes initial data.
	 * @param {string} address The target address.
	 * @param {number} port The target port.
	 * @param {boolean} socks True if SOCKS5 proxy should be used.
	 * @param {boolean} http True if HTTP proxy should be used (for SOCKS5).
	 * @returns {Promise<import('cloudflare:sockets').Socket>} The connected TCP socket.
	 */
	async function connectAndWrite(address, port, socks = false, http = false) {
		log(`尝试连接到 ${address}:${port}`);
		let tcpSocket;
		if (socks) {
			tcpSocket = http ? await httpConnect(address, port, log) : await socks5Connect(addressType, address, port, log);
		} else {
			tcpSocket = connect({ hostname: address, port: port });
		}

		remoteSocketWapper.value = tcpSocket;
		log(`已连接到 ${address}:${port}`);

		const writer = tcpSocket.writable.getWriter();
		await writer.write(rawClientData); // Write initial client data
		writer.releaseLock();
		return tcpSocket;
	}

	/**
	 * Retry function: attempts to redirect IP if Cloudflare's TCP Socket doesn't receive data.
	 * This may happen due to network issues or connection failures.
	 */
	async function retryConnection() {
		log('尝试重试连接...');
		let newTcpSocket;
		let currentPortRemote = portRemote; // Use a local variable for portRemote

		if (enableSocks) {
			// If SOCKS5 is enabled, retry connection via SOCKS5 proxy
			newTcpSocket = await connectAndWrite(addressRemote, currentPortRemote, true, enableHttp);
		} else {
			// Otherwise, try using a preset proxy IP or the original address
			let effectiveProxyIP = proxyIP;
			if (!effectiveProxyIP || effectiveProxyIP === '') {
				effectiveProxyIP = atob('UFJPWFlJUC50cDEuMDkwMjI3Lnh5eg==');
			} else if (effectiveProxyIP.includes(']:')) {
				currentPortRemote = Number(effectiveProxyIP.split(']:')[1]) || currentPortRemote;
				effectiveProxyIP = effectiveProxyIP.split(']:')[0] + "]";
			} else if (effectiveProxyIP.split(':').length === 2) {
				currentPortRemote = Number(effectiveProxyIP.split(':')[1]) || currentPortRemote;
				effectiveProxyIP = effectiveProxyIP.split(':')[0];
			}
			if (effectiveProxyIP.includes('.tp')) currentPortRemote = Number(effectiveProxyIP.split('.tp')[1].split('.')[0]) || currentPortRemote;

			newTcpSocket = await connectAndWrite(effectiveProxyIP.toLowerCase() || addressRemote, currentPortRemote);
		}

		// Close WebSocket when the new TCP socket closes (for cleanup)
		newTcpSocket.closed.catch(error => {
			console.log('retry tcpSocket closed error', error);
		}).finally(() => {
			safeCloseWebSocket(webSocket);
		});

		// Establish data flow from the new remote Socket to WebSocket
		remoteSocketToWS(newTcpSocket, webSocket, 维列斯ResponseHeader, null, log);
	}

	let useSocks = false;
	if (go2Socks5s.length > 0 && enableSocks) {
		useSocks = await useSocks5Pattern(addressRemote);
	}

	// First attempt to connect to the remote server
	let tcpSocket = await connectAndWrite(addressRemote, portRemote, useSocks, enableHttp);

	// When the remote Socket is ready, pipe its readable stream to the WebSocket
	// If connection fails or no data is received, the retry function will be called.
	remoteSocketToWS(tcpSocket, webSocket, 维列斯ResponseHeader, retryConnection, log);
}

/**
 * Creates a readable stream from a WebSocket server.
 * This stream allows consuming data sent from the WebSocket client.
 * @param {WebSocket} webSocketServer The WebSocket server instance.
 * @param {string} earlyDataHeader The 'sec-websocket-protocol' header for 0-RTT data.
 * @param {(string)=> void} log Logging function.
 * @returns {ReadableStream<Uint8Array>} A readable stream of WebSocket messages.
 */
function makeReadableWebSocketStream(webSocketServer, earlyDataHeader, log) {
	let readableStreamCancel = false; // Flag to track if the readable stream has been cancelled

	const stream = new ReadableStream({
		start(controller) {
			// Listen for 'message' events from the WebSocket
			webSocketServer.addEventListener('message', (event) => {
				if (readableStreamCancel) {
					return; // If stream is cancelled, ignore new messages
				}
				const message = event.data;
				controller.enqueue(message); // Enqueue the message into the stream
			});

			// Listen for 'close' events from the WebSocket
			// This means the client closed its end of the stream.
			webSocketServer.addEventListener('close', () => {
				safeCloseWebSocket(webSocketServer); // Close the server-side WebSocket
				if (readableStreamCancel) {
					return;
				}
				controller.close(); // Close the stream controller
			});

			// Listen for 'error' events from the WebSocket
			webSocketServer.addEventListener('error', (err) => {
				log('WebSocket 服务器发生错误');
				controller.error(err); // Propagate the error to the stream controller
			});

			// Handle WebSocket 0-RTT (Zero Round-Trip Time) early data
			const { earlyData, error } = base64ToArrayBuffer(earlyDataHeader);
			if (error) {
				controller.error(error); // If decoding early data fails, propagate the error
			} else if (earlyData) {
				controller.enqueue(earlyData); // Enqueue early data into the stream
			}
		},
		pull(controller) {
			// This method can be used for backpressure, but not implemented here.
		},
		cancel(reason) {
			// Called when the stream is cancelled (e.g., due to an error in the WritableStream)
			if (readableStreamCancel) {
				return;
			}
			log(`可读流被取消，原因是 ${reason}`);
			readableStreamCancel = true;
			safeCloseWebSocket(webSocketServer); // Safely close the WebSocket server
		}
	});

	return stream;
}

/**
 * Parses the Vless protocol header data.
 * @param {ArrayBuffer} vlessBuffer The raw Vless protocol header data.
 * @param {string} userID The user ID for authentication.
 * @returns {{hasError: boolean, message?: string, addressType?: number, portRemote?: number, addressRemote?: string, rawDataIndex?: number, 维列斯Version?: Uint8Array, isUDP?: boolean}} Parsing result.
 */
function process维列斯Header(vlessBuffer, userID) {
	// Check if data length is sufficient (at least 24 bytes for header)
	if (vlessBuffer.byteLength < 24) {
		return { hasError: true, message: '无效数据: 头部长度不足' };
	}

	// Parse Vless protocol version (first byte)
	const version = new Uint8Array(vlessBuffer.slice(0, 1));

	// Validate user ID (next 16 bytes)
	const userIDArray = new Uint8Array(vlessBuffer.slice(1, 17));
	const userIDString = unsafeStringify(userIDArray); // Use unsafeStringify for performance, validation done later

	// Check if the extracted user ID matches the provided userID or userIDLow
	const isValidUser = (userIDString === userID || userIDString === userIDLow);

	if (!isValidUser) {
		return { hasError: true, message: `无效用户ID: ${userIDString}` };
	}

	// Get options length (17th byte)
	const optLength = new Uint8Array(vlessBuffer.slice(17, 18))[0];

	// Parse command (1 byte after options)
	// 0x01: TCP, 0x02: UDP, 0x03: MUX
	const commandIndex = 18 + optLength;
	if (vlessBuffer.byteLength < commandIndex + 1) {
		return { hasError: true, message: '无效数据: 命令字节缺失' };
	}
	const command = new Uint8Array(vlessBuffer.slice(commandIndex, commandIndex + 1))[0];

	let isUDP = false;
	if (command === 1) {
		// TCP command
	} else if (command === 2) {
		// UDP command
		isUDP = true;
	} else {
		return { hasError: true, message: `不支持的命令: ${command} (仅支持 01-tcp, 02-udp, 03-mux)` };
	}

	// Parse port (2 bytes after command)
	const portIndex = commandIndex + 1;
	if (vlessBuffer.byteLength < portIndex + 2) {
		return { hasError: true, message: '无效数据: 端口字节缺失' };
	}
	const portRemote = new DataView(vlessBuffer.slice(portIndex, portIndex + 2)).getUint16(0, false); // Big-endian

	// Parse address type (1 byte after port)
	const addressTypeIndex = portIndex + 2;
	if (vlessBuffer.byteLength < addressTypeIndex + 1) {
		return { hasError: true, message: '无效数据: 地址类型字节缺失' };
	}
	const addressType = new Uint8Array(vlessBuffer.slice(addressTypeIndex, addressTypeIndex + 1))[0];

	let addressRemote = '';
	let addressLength = 0;
	let rawDataIndex = 0;

	// Parse address based on address type
	switch (addressType) {
		case 1: // IPv4
			addressLength = 4;
			rawDataIndex = addressTypeIndex + 1 + addressLength;
			if (vlessBuffer.byteLength < rawDataIndex) {
				return { hasError: true, message: '无效数据: IPv4 地址缺失' };
			}
			addressRemote = new Uint8Array(vlessBuffer.slice(addressTypeIndex + 1, rawDataIndex)).join('.');
			break;
		case 2: // Domain
			addressLength = new Uint8Array(vlessBuffer.slice(addressTypeIndex + 1, addressTypeIndex + 2))[0];
			rawDataIndex = addressTypeIndex + 2 + addressLength;
			if (vlessBuffer.byteLength < rawDataIndex) {
				return { hasError: true, message: '无效数据: 域名地址缺失' };
			}
			addressRemote = new TextDecoder().decode(vlessBuffer.slice(addressTypeIndex + 2, rawDataIndex));
			break;
		case 3: // IPv6
			addressLength = 16;
			rawDataIndex = addressTypeIndex + 1 + addressLength;
			if (vlessBuffer.byteLength < rawDataIndex) {
				return { hasError: true, message: '无效数据: IPv6 地址缺失' };
			}
			const ipv6Array = new Uint8Array(vlessBuffer.slice(addressTypeIndex + 1, rawDataIndex));
			addressRemote = Array.from(ipv6Array).map((byte, i) => {
				if (i % 2 === 0) {
					return byteToHex[byte] + byteToHex[ipv6Array[i + 1]];
				}
				return '';
			}).filter(s => s).join(':');
			break;
		default:
			return { hasError: true, message: `不支持的地址类型: ${addressType}` };
	}

	return {
		hasError: false,
		addressType,
		portRemote,
		addressRemote,
		rawDataIndex,
		维列斯Version: version,
		isUDP,
	};
}

/**
 * Converts a Uint8Array to a string without decoding, used for UUID comparison.
 * @param {Uint8Array} arr The Uint8Array to convert.
 * @returns {string} The string representation.
 */
function unsafeStringify(arr) {
	return String.fromCharCode.apply(null, arr);
}

/**
 * Decodes a base64 string to an ArrayBuffer.
 * @param {string} base64Str The base64 string.
 * @returns {{earlyData?: ArrayBuffer, error?: Error}} Result object.
 */
function base64ToArrayBuffer(base64Str) {
	try {
		const binaryString = atob(base64Str);
		const len = binaryString.length;
		const bytes = new Uint8Array(len);
		for (let i = 0; i < len; i++) {
			bytes[i] = binaryString.charCodeAt(i);
		}
		return { earlyData: bytes.buffer };
	} catch (error) {
		return { error };
	}
}

/**
 * Safely closes a WebSocket connection if it's open or closing.
 * @param {WebSocket} ws The WebSocket instance.
 */
function safeCloseWebSocket(ws) {
	try {
		if (ws.readyState === WS_READY_STATE_OPEN || ws.readyState === WS_READY_STATE_CLOSING) {
			ws.close();
		}
	} catch (error) {
		console.error('Error closing WebSocket:', error);
	}
}

/**
 * Handles DNS queries over UDP.
 * @param {Uint8Array} rawClientData The raw DNS query data.
 * @param {WebSocket} webSocket The client WebSocket.
 * @param {Uint8Array | null} 维列斯ResponseHeader Vless response header (if any).
 * @param {(string)=> void} log Logging function.
 */
async function handleDNSQuery(rawClientData, webSocket, 维列斯ResponseHeader, log) {
	try {
		// DNS over UDP is typically on port 53
		const dnsServer = '1.1.1.1'; // Example DNS server
		const dnsPort = 53;

		log(`处理 DNS 查询到 ${dnsServer}:${dnsPort}`);

		const udpSocket = connect({ hostname: dnsServer, port: dnsPort, protocol: 'udp' });

		const writer = udpSocket.writable.getWriter();
		await writer.write(rawClientData);
		writer.releaseLock();

		const reader = udpSocket.readable.getReader();
		const { value: dnsResponse } = await reader.read();
		reader.releaseLock();

		if (dnsResponse) {
			const responseBuffer = new Uint8Array(dnsResponse);
			const finalResponse = 维列斯ResponseHeader ? new Uint8Array([...维列斯ResponseHeader, ...responseBuffer]) : responseBuffer;
			webSocket.send(finalResponse);
		} else {
			throw new Error('未收到 DNS 响应');
		}
	} catch (error) {
		log('DNS 查询处理错误:', error);
		safeCloseWebSocket(webSocket);
	}
}

/**
 * Establishes a SOCKS5 connection to a remote server.
 * @param {number} addressType Type of the remote address (IPv4, Domain, IPv6).
 * @param {string} addressRemote Remote address.
 * @param {number} portRemote Remote port.
 * @param {(string)=> void} log Logging function.
 * @returns {Promise<import('cloudflare:sockets').Socket>} The connected SOCKS5 TCP socket.
 */
async function socks5Connect(addressType, addressRemote, portRemote, log) {
	log(`尝试通过 SOCKS5 连接到 ${addressRemote}:${portRemote}`);
	const socks5Host = parsedSocks5Address.hostname;
	const socks5Port = parsedSocks5Address.port;
	const socks5Username = parsedSocks5Address.username;
	const socks5Password = parsedSocks5Address.password;

	const tcpSocket = connect({ hostname: socks5Host, port: socks5Port });
	const writer = tcpSocket.writable.getWriter();
	const reader = tcpSocket.readable.getReader();

	// SOCKS5 Handshake: Version and Authentication Method Selection
	// +----+----------+----------+
	// |VER | NMETHODS | METHODS  |
	// +----+----------+----------+
	// | 0x05 |    1     | 1 to 255 |
	// +----+----------+----------+
	// Methods: 0x00 NO AUTHENTICATION REQUIRED, 0x02 USERNAME/PASSWORD
	const authMethod = (socks5Username && socks5Password) ? 0x02 : 0x00;
	await writer.write(new Uint8Array([0x05, 0x01, authMethod]));

	const { value: authResponse } = await reader.read();
	if (!authResponse || authResponse.byteLength < 2 || authResponse[0] !== 0x05 || authResponse[1] !== authMethod) {
		throw new Error('SOCKS5 认证方法协商失败');
	}

	// SOCKS5 Authentication (if required)
	if (authMethod === 0x02) {
		// +----+--------+----------+--------+----------+
		// |VER | ULEN   | UNAME    | PLEN   | PASSWD   |
		// +----+--------+----------+--------+----------+
		// | 0x01 | 1      | 1 to 255 | 1      | 1 to 255 |
		// +----+--------+----------+--------+----------+
		const usernameBytes = new TextEncoder().encode(socks5Username);
		const passwordBytes = new TextEncoder().encode(socks5Password);
		const authRequest = new Uint8Array([
			0x01,
			usernameBytes.length, ...usernameBytes,
			passwordBytes.length, ...passwordBytes
		]);
		await writer.write(authRequest);

		const { value: authStatusResponse } = await reader.read();
		if (!authStatusResponse || authStatusResponse.byteLength < 2 || authStatusResponse[0] !== 0x01 || authStatusResponse[1] !== 0x00) {
			throw new Error('SOCKS5 用户名/密码认证失败');
		}
	}

	// SOCKS5 Connection Request
	// +----+-----+-------+------+----------+----------+
	// |VER | CMD | RSV   | ATYP | DST.ADDR | DST.PORT |
	// +----+-----+-------+------+----------+----------+
	// | 0x05 | 0x01 | 0x00  |      |          |          |
	// +----+-----+-------+------+----------+----------+
	// CMD: 0x01 CONNECT
	// ATYP: 0x01 IPv4, 0x03 Domain, 0x04 IPv6
	let connectRequest;
	let addressBytes;

	switch (addressType) {
		case 1: // IPv4
			addressBytes = new Uint8Array(addressRemote.split('.').map(Number));
			connectRequest = new Uint8Array([0x05, 0x01, 0x00, 0x01, ...addressBytes, (portRemote >> 8) & 0xFF, portRemote & 0xFF]);
			break;
		case 2: // Domain
			addressBytes = new TextEncoder().encode(addressRemote);
			connectRequest = new Uint8Array([0x05, 0x01, 0x00, 0x03, addressBytes.length, ...addressBytes, (portRemote >> 8) & 0xFF, portRemote & 0xFF]);
			break;
		case 3: // IPv6
			addressBytes = new Uint8Array(addressRemote.split(':').flatMap(segment => {
				const val = parseInt(segment, 16);
				return [(val >> 8) & 0xFF, val & 0xFF];
			}));
			connectRequest = new Uint8Array([0x05, 0x01, 0x00, 0x04, ...addressBytes, (portRemote >> 8) & 0xFF, portRemote & 0xFF]);
			break;
		default:
			throw new Error(`不支持的 SOCKS5 地址类型: ${addressType}`);
	}

	await writer.write(connectRequest);

	const { value: connectResponse } = await reader.read();
	if (!connectResponse || connectResponse.byteLength < 4 || connectResponse[0] !== 0x05 || connectResponse[1] !== 0x00) {
		throw new Error(`SOCKS5 连接请求失败: ${connectResponse ? connectResponse[1] : '无响应'}`);
	}

	writer.releaseLock();
	reader.releaseLock();
	return tcpSocket;
}

/**
 * Establishes an HTTP proxy connection to a remote server.
 * @param {string} addressRemote Remote address.
 * @param {number} portRemote Remote port.
 * @param {(string)=> void} log Logging function.
 * @returns {Promise<import('cloudflare:sockets').Socket>} The connected HTTP proxy TCP socket.
 */
async function httpConnect(addressRemote, portRemote, log) {
	log(`尝试通过 HTTP 代理连接到 ${addressRemote}:${portRemote}`);
	const httpProxyHost = parsedSocks5Address.hostname;
	const httpProxyPort = parsedSocks5Address.port;
	const httpProxyUsername = parsedSocks5Address.username;
	const httpProxyPassword = parsedSocks5Address.password;

	const tcpSocket = connect({ hostname: httpProxyHost, port: httpProxyPort });
	const writer = tcpSocket.writable.getWriter();
	const reader = tcpSocket.readable.getReader();

	let authHeader = '';
	if (httpProxyUsername && httpProxyPassword) {
		const authString = btoa(`${httpProxyUsername}:${httpProxyPassword}`);
		authHeader = `Proxy-Authorization: Basic ${authString}\r\n`;
	}

	const connectRequest = new TextEncoder().encode(
		`CONNECT ${addressRemote}:${portRemote} HTTP/1.1\r\n` +
		`Host: ${addressRemote}:${portRemote}\r\n` +
		authHeader +
		`Proxy-Connection: keep-alive\r\n\r\n`
	);

	await writer.write(connectRequest);

	// Read HTTP proxy response (e.g., HTTP/1.1 200 Connection established)
	let responseBuffer = new Uint8Array();
	let headersEnd = -1;
	while (headersEnd === -1) {
		const { value, done } = await reader.read();
		if (done) break;
		responseBuffer = new Uint8Array([...responseBuffer, ...value]);
		headersEnd = findCRLFCRLF(responseBuffer);
	}

	if (headersEnd === -1) {
		throw new Error('HTTP 代理响应头不完整');
	}

	const responseHeaders = new TextDecoder().decode(responseBuffer.slice(0, headersEnd));
	if (!responseHeaders.includes('200 Connection established')) {
		throw new Error(`HTTP 代理连接失败: ${responseHeaders.split('\r\n')[0]}`);
	}

	writer.releaseLock();
	reader.releaseLock();
	return tcpSocket;
}

/**
 * Finds the CRLF CRLF sequence in a Uint8Array.
 * @param {Uint8Array} buffer The buffer to search.
 * @returns {number} The index of the first byte of CRLF CRLF, or -1 if not found.
 */
function findCRLFCRLF(buffer) {
	for (let i = 0; i < buffer.length - 3; i++) {
		if (buffer[i] === 0x0D && buffer[i + 1] === 0x0A && buffer[i + 2] === 0x0D && buffer[i + 3] === 0x0A) {
			return i + 4;
		}
	}
	return -1;
}

/**
 * Pipes data from a remote TCP socket to a WebSocket.
 * Includes a retry mechanism if the remote socket doesn't receive data initially.
 * @param {import('cloudflare:sockets').Socket} remoteSocket The remote TCP socket.
 * @param {WebSocket} webSocket The client WebSocket.
 * @param {Uint8Array} 维列斯ResponseHeader Vless response header.
 * @param {(() => Promise<void>) | null} retryCallback Callback function to retry connection.
 * @param {(string)=> void} log Logging function.
 */
async function remoteSocketToWS(remoteSocket, webSocket, 维列斯ResponseHeader, retryCallback, log) {
	let receivedData = false;
	try {
		// Write Vless response header first
		if (维列斯ResponseHeader) {
			webSocket.send(维列斯ResponseHeader);
		}

		const reader = remoteSocket.readable.getReader();
		while (true) {
			const { done, value } = await reader.read();
			if (done) {
				break; // Remote socket closed
			}
			if (value) {
				receivedData = true;
				webSocket.send(value); // Send data to WebSocket
			}
		}
	} catch (error) {
		log('remoteSocketToWS 管道错误', error);
		if (!receivedData && retryCallback) {
			// If no data was received and a retry callback exists, attempt to retry
			log('远程套接字未收到数据，尝试重试...');
			await retryCallback();
		} else {
			safeCloseWebSocket(webSocket); // Close WebSocket on error
		}
	} finally {
		// Ensure both ends are closed
		safeCloseWebSocket(webSocket);
		remoteSocket.close();
	}
}

/**
 * Parses a SOCKS5 address string into its components.
 * Supports formats like `hostname:port`, `username:password@hostname:port`.
 * @param {string} address The SOCKS5 address string.
 * @returns {{hostname: string, port: number, username?: string, password?: string}} Parsed components.
 */
function socks5AddressParser(address) {
	let hostname = '';
	let port = 1080; // Default SOCKS5 port
	let username = '';
	let password = '';

	const atIndex = address.indexOf('@');
	if (atIndex !== -1) {
		const authPart = address.substring(0, atIndex);
		const hostPortPart = address.substring(atIndex + 1);

		const colonIndex = authPart.indexOf(':');
		if (colonIndex !== -1) {
			username = authPart.substring(0, colonIndex);
			password = authPart.substring(colonIndex + 1);
		} else {
			username = authPart; // Assume no password if only username is present before @
		}

		const hostPortSplit = hostPortPart.split(':');
		hostname = hostPortSplit[0];
		if (hostPortSplit.length > 1) {
			port = parseInt(hostPortSplit[1], 10);
		}
	} else {
		const hostPortSplit = address.split(':');
		hostname = hostPortSplit[0];
		if (hostPortSplit.length > 1) {
			port = parseInt(hostPortSplit[1], 10);
		}
	}

	if (!hostname) {
		throw new Error('SOCKS5 地址解析失败: 缺少主机名');
	}
	if (isNaN(port)) {
		throw new Error('SOCKS5 地址解析失败: 无效端口');
	}

	return { hostname, port, username, password };
}

/**
 * Processes a string input, splitting it by newline and removing empty entries.
 * @param {string | undefined} input The input string.
 * @returns {string[]} An array of processed strings.
 */
async function 整理(input) {
	if (!input) return [];
	return input.split('\n').map(s => s.trim()).filter(s => s !== '');
}

/**
 * Generates a dynamic UUID based on a key/token and current time.
 * @param {string} key The key or token to use for UUID generation.
 * @returns {Promise<[string, string, string]>} An array containing the generated UUID, its lowercase version, and the timestamp string.
 */
async function 生成动态UUID(key) {
	const now = new Date();
	const utcOffset = 8; // Beijing time offset in hours
	now.setUTCHours(now.getUTCHours() + utcOffset);

	const year = now.getUTCFullYear();
	const month = (now.getUTCMonth() + 1).toString().padStart(2, '0');
	const day = now.getUTCDate().toString().padStart(2, '0');
	const hour = now.getUTCHours().toString().padStart(2, '0');

	const dateStr = `${year}-${month}-${day}`;
	const timeStr = `${hour}`;

	const hashInput = `${key}-${dateStr}-${timeStr}`;
	const hash = await 双重哈希(hashInput);

	const uuid = [
		hash.slice(0, 8),
		hash.slice(8, 12),
		hash.slice(12, 16),
		hash.slice(16, 20),
		hash.slice(20)
	].join('-');

	return [uuid, uuid.toLowerCase(), timeStr];
}

/**
 * Checks if a string is a valid UUID format.
 * @param {string} uuid The string to check.
 * @returns {boolean} True if it's a valid UUID, false otherwise.
 */
function isValidUUID(uuid) {
	const uuidRegex = /^[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}$/;
	return uuidRegex.test(uuid);
}

/**
 * Computes the MD5 hash of a string twice.
 * @param {string} str The input string.
 * @returns {Promise<string>} The double MD5 hash in hexadecimal format.
 */
async function 双重哈希(str) {
	const encoder = new TextEncoder();
	const data = encoder.encode(str);
	const hashBuffer1 = await crypto.subtle.digest('MD5', data);
	const hashArray1 = Array.from(new Uint8Array(hashBuffer1));
	const hexHash1 = hashArray1.map(b => b.toString(16).padStart(2, '0')).join('');

	const data2 = encoder.encode(hexHash1);
	const hashBuffer2 = await crypto.subtle.digest('MD5', data2);
	const hashArray2 = Array.from(new Uint8Array(hashBuffer2));
	const hexHash2 = hashArray2.map(b => b.toString(16).padStart(2, '0')).join('');
	return hexHash2;
}

/**
 * Generates Vless configuration information.
 * @param {string} id User ID.
 * @param {string} host Hostname.
 * @param {string} subType Subscription type.
 * @param {string} userAgent User-Agent string.
 * @param {string} RproxyIPFlag Random proxy IP flag.
 * @param {URL} url The request URL.
 * @param {string} fakeUserID Fake user ID for obfuscation.
 * @param {string} fakeHostName Fake hostname for obfuscation.
 * @param {Env} env Environment variables.
 * @returns {Promise<string>} The generated Vless configuration.
 */
async function 生成配置信息(id, host, subType, userAgent, RproxyIPFlag, url, fakeUserID, fakeHostName, env) {
	let vlessLinks = [];
	let remarks = [];

	// Determine effective proxy IP based on RproxyIPFlag
	let effectiveProxyIP = RproxyIPFlag === 'true' ? proxyIP : host;
	if (effectiveProxyIP.includes('workers.dev')) {
		effectiveProxyIP = proxyIP;
	}

	// Add the main Vless link
	vlessLinks.push(生成VlessLink(id, effectiveProxyIP, host, path, FileName, noTLS, allowInsecure));
	remarks.push(FileName);

	// Add dynamic UUID link if applicable
	if (动态UUID) {
		const dynamicRemark = `动态UUID-${userIDTime}`;
		vlessLinks.push(生成VlessLink(userID, effectiveProxyIP, host, path, dynamicRemark, noTLS, allowInsecure));
		remarks.push(dynamicRemark);
	}

	// Add fake Vless link for obfuscation
	const fakeRemark = `Fake-${fakeHostName.split('.')[0]}`;
	vlessLinks.push(生成VlessLink(fakeUserID, fakeHostName, host, path, fakeRemark, noTLS, allowInsecure));
	remarks.push(fakeRemark);

	// Add links from env.LINK
	for (const l of link) {
		const parts = l.split(',');
		if (parts.length >= 2) {
			const linkRemark = parts[0];
			const linkURL = parts[1];
			// Assuming linkURL is a full Vless link or similar,
			// or we need to construct it based on some logic.
			// For simplicity, let's assume it's a direct Vless link for now.
			// If it's just a URL, it needs more context to be a Vless link.
			// For this example, let's just add it as is if it's a Vless link.
			if (linkURL.startsWith('vless://')) {
				vlessLinks.push(linkURL);
				remarks.push(linkRemark);
			} else {
				// If it's not a vless link, it might be a remark and an address/port
				// This part needs clarification on how env.LINK is structured for non-vless links
				// For now, we'll skip non-vless links from env.LINK
				console.warn(`Skipping non-vless link from env.LINK: ${linkURL}`);
			}
		}
	}

	// Add Vless links from addresses, addressesapi, addressesnotls, addressesnotlsapi
	const processAddresses = (addrs, isApi, isNoTLS) => {
		for (const addr of addrs) {
			let currentHost = addr;
			let currentPort = 443;
			let currentRemark = `${FileName}-${addr}`;
			let currentPath = path;

			// Handle port in address string
			if (addr.includes(':')) {
				const parts = addr.split(':');
				currentHost = parts[0];
				currentPort = parseInt(parts[1], 10);
			}

			// For API addresses, add a specific path
			if (isApi) {
				currentPath = `/api${path}`;
			}

			// Use the original host for SNI if it's a workers.dev domain, otherwise use currentHost
			const sniHost = host.includes('workers.dev') ? host : currentHost;

			vlessLinks.push(生成VlessLink(id, currentHost, sniHost, currentPath, currentRemark, isNoTLS ? 'true' : noTLS, allowInsecure, currentPort));
			remarks.push(currentRemark);
		}
	};

	processAddresses(addresses, false, false);
	processAddresses(addressesapi, true, false);
	processAddresses(addressesnotls, false, true);
	processAddresses(addressesnotlsapi, true, true);

	// Add Vless links from addressescsv
	for (const csvAddr of addressescsv) {
		const parts = csvAddr.split(',');
		if (parts.length > remarkIndex) {
			const ip = parts[0];
			const remark = parts[remarkIndex];
			const port = parts[1] ? parseInt(parts[1], 10) : 443; // Assuming port is at index 1
			const sniHost = host.includes('workers.dev') ? host : ip; // Use original host for SNI if workers.dev
			vlessLinks.push(生成VlessLink(id, ip, sniHost, path, remark, noTLS, allowInsecure, port));
			remarks.push(remark);
		}
	}

	let configContent = '';
	if (subParams.includes(subType)) {
		// If it's a subscription request, format as base64 encoded Vless links
		configContent = btoa(vlessLinks.join('\n'));
	} else if (userAgent.includes('mozilla')) {
		// If it's a browser, display as an HTML page
		configContent = `
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>${FileName}</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 20px; background-color: #f0f2f5; color: #333; }
.container { max-width: 800px; margin: 20px auto; background-color: #fff; padding: 30px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1); }
h1 { color: #0056b3; text-align: center; margin-bottom: 30px; }
.section { background-color: #e9ecef; padding: 20px; border-radius: 8px; margin-bottom: 20px; }
.section h2 { margin-top: 0; color: #0056b3; }
pre { background-color: #e9ecef; padding: 15px; border-radius: 8px; overflow-x: auto; white-space: pre-wrap; word-break: break-all; }
.link-list { list-style: none; padding: 0; }
.link-list li { margin-bottom: 10px; }
.link-list a { color: #007bff; text-decoration: none; word-break: break-all; }
.link-list a:hover { text-decoration: underline; }
.footer { text-align: center; margin-top: 40px; font-size: 0.9em; color: #666; }
.copy-button {
    background-color: #007bff;
    color: white;
    border: none;
    padding: 8px 15px;
    border-radius: 5px;
    cursor: pointer;
    font-size: 0.9em;
    margin-left: 10px;
}
.copy-button:hover {
    background-color: #0056b3;
}
.message-box {
    position: fixed;
    bottom: 20px;
    left: 50%;
    transform: translateX(-50%);
    background-color: #4CAF50;
    color: white;
    padding: 10px 20px;
    border-radius: 5px;
    z-index: 1000;
    display: none;
}
</style>
</head>
<body>
<div class="container">
    <h1>${FileName} 订阅配置</h1>
    <div class="section">
        <h2>Vless 链接</h2>
        <ul class="link-list">
            ${vlessLinks.map((link, index) => `<li>${remarks[index]}: <a href="${link}">${link}</a> <button class="copy-button" onclick="copyToClipboard('${link}')">复制</button></li>`).join('')}
        </ul>
    </div>
    <div class="section">
        <h2>订阅链接 (Base64)</h2>
        <p>将此链接导入您的客户端以获取所有节点。</p>
        <pre id="subscriptionLink">${subProtocol}://${subConverter}/sub?target=${subType}&url=${encodeURIComponent(url.origin + url.pathname)}&insert=true&emoji=${subEmoji}&config=${encodeURIComponent(subConfig)}&scv=${SCV}</pre>
        <button class="copy-button" onclick="copyToClipboard(document.getElementById('subscriptionLink').innerText)">复制订阅链接</button>
    </div>
    <div class="footer">
        ${SUBSCRIPTION_FOOTER_MESSAGE}
    </div>
</div>
<div id="messageBox" class="message-box"></div>

<script>
function copyToClipboard(text) {
    const textarea = document.createElement('textarea');
    textarea.value = text;
    document.body.appendChild(textarea);
    textarea.select();
    document.execCommand('copy');
    document.body.removeChild(textarea);

    const messageBox = document.getElementById('messageBox');
    messageBox.innerText = '已复制到剪贴板！';
    messageBox.style.display = 'block';
    setTimeout(() => {
        messageBox.style.display = 'none';
    }, 2000);
}
</script>
</body>
</html>
`;
	} else {
		// Default to base64 encoded Vless links for other clients
		configContent = btoa(vlessLinks.join('\n'));
	}

	return configContent;
}

/**
 * Generates a Vless link string.
 * @param {string} id User ID.
 * @param {string} address The target address.
 * @param {string} sni SNI hostname.
 * @param {string} path Path for WebSocket.
 * @param {string} remark Remark/name of the link.
 * @param {string} noTLSFlag Flag to disable TLS.
 * @param {string} allowInsecureFlag Flag to allow insecure TLS.
 * @param {number} port The target port (default 443).
 * @returns {string} The Vless link.
 */
function 生成VlessLink(id, address, sni, path, remark, noTLSFlag, allowInsecureFlag, port = 443) {
	let tlsSettings = '';
	if (noTLSFlag === 'true') {
		tlsSettings = `security=none&type=ws&host=${sni}&path=${encodeURIComponent(path)}`;
	} else {
		tlsSettings = `security=tls&type=ws&sni=${sni}&fp=random&pbk=random&flow=xtls-rprx-vision&encryption=none&host=${sni}&path=${encodeURIComponent(path)}${allowInsecureFlag}`;
	}
	const emojiRemark = subEmoji === 'true' ? `🚀${remark}` : remark;
	return `vless://${id}@${address}:${port}?${tlsSettings}#${encodeURIComponent(emojiRemark)}`;
}

/**
 * Proxies a given URL.
 * @param {string} targetURL The URL to proxy.
 * @param {URL} requestURL The original request URL.
 * @returns {Promise<Response>} The proxied response.
 */
async function 代理URL(targetURL, requestURL) {
	const newURL = new URL(targetURL);
	newURL.pathname = requestURL.pathname;
	newURL.search = requestURL.search;

	const newRequest = new Request(newURL.toString(), {
		method: requestURL.method,
		headers: requestURL.headers,
		body: requestURL.body,
		redirect: 'follow',
	});

	return fetch(newRequest);
}

/**
 * Handles KV storage operations and serves an HTML editing page.
 * @param {Request} request The incoming request.
 * @param {Env} env The environment variables.
 * @returns {Promise<Response>} The HTML response or a JSON response for API calls.
 */
async function KV(request, env) {
	const url = new URL(request.url);
	const path = url.pathname;
	const token = env.KV_TOKEN; // Secret token for KV access

	if (!token) {
		return new Response('请设置 KV_TOKEN 环境变量以启用 KV 编辑功能。', { status: 403 });
	}

	const authHeader = request.headers.get('Authorization');
	if (authHeader !== `Bearer ${token}`) {
		return new Response('未经授权的访问。', { status: 401 });
	}

	const key = url.searchParams.get('key');
	const value = url.searchParams.get('value');
	const action = url.searchParams.get('action');

	if (request.method === 'POST') {
		const formData = await request.formData();
		const postKey = formData.get('key');
		const postValue = formData.get('value');
		const postAction = formData.get('action');

		if (!env.KV_NAMESPACE) {
			return new Response(JSON.stringify({ success: false, message: 'KV_NAMESPACE 未配置' }), { status: 500 });
		}

		try {
			if (postAction === 'put' && postKey && postValue) {
				await env.KV_NAMESPACE.put(postKey, postValue);
				return new Response(JSON.stringify({ success: true, message: `键 '${postKey}' 已保存。` }), { status: 200 });
			} else if (postAction === 'delete' && postKey) {
				await env.KV_NAMESPACE.delete(postKey);
				return new Response(JSON.stringify({ success: true, message: `键 '${postKey}' 已删除。` }), { status: 200 });
			} else {
				return new Response(JSON.stringify({ success: false, message: '无效的 KV 操作。' }), { status: 400 });
			}
		} catch (e) {
			return new Response(JSON.stringify({ success: false, message: `KV 操作失败: ${e.message}` }), { status: 500 });
		}
	} else if (request.method === 'GET' && action === 'get' && key) {
		if (!env.KV_NAMESPACE) {
			return new Response(JSON.stringify({ success: false, message: 'KV_NAMESPACE 未配置' }), { status: 500 });
		}
		try {
			const val = await env.KV_NAMESPACE.get(key);
			return new Response(JSON.stringify({ success: true, key, value: val }), { status: 200 });
		} catch (e) {
			return new Response(JSON.stringify({ success: false, message: `获取键 '${key}' 失败: ${e.message}` }), { status: 500 });
		}
	} else if (request.method === 'GET') {
		// Serve the HTML editing page
		const html = `
<!DOCTYPE html>
<html>
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>KV 编辑器</title>
<style>
body { font-family: -apple-system, BlinkMacSystemFont, "Segoe UI", Roboto, Helvetica, Arial, sans-serif; margin: 0; padding: 20px; background-color: #f0f2f5; color: #333; }
.container { max-width: 800px; margin: 20px auto; background-color: #fff; padding: 30px; border-radius: 12px; box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1); }
h1 { color: #0056b3; text-align: center; margin-bottom: 30px; }
.form-group { margin-bottom: 15px; }
label { display: block; margin-bottom: 5px; font-weight: bold; }
input[type="text"], textarea { width: 100%; padding: 10px; border: 1px solid #ccc; border-radius: 5px; box-sizing: border-box; }
textarea { min-height: 100px; resize: vertical; }
button {
    background-color: #007bff;
    color: white;
    border: none;
    padding: 10px 20px;
    border-radius: 5px;
    cursor: pointer;
    font-size: 1em;
    margin-right: 10px;
}
button:hover { background-color: #0056b3; }
.message { margin-top: 20px; padding: 10px; border-radius: 5px; }
.message.success { background-color: #d4edda; color: #155724; border-color: #c3e6cb; }
.message.error { background-color: #f8d7da; color: #721c24; border-color: #f5c6cb; }
</style>
</head>
<body>
<div class="container">
    <h1>KV 编辑器</h1>
    <div class="form-group">
        <label for="kvKey">键 (Key):</label>
        <input type="text" id="kvKey" name="key">
    </div>
    <div class="form-group">
        <label for="kvValue">值 (Value):</label>
        <textarea id="kvValue" name="value"></textarea>
    </div>
    <button onclick="performKvAction('put')">保存</button>
    <button onclick="performKvAction('get')">获取</button>
    <button onclick="performKvAction('delete')">删除</button>
    <div id="message" class="message" style="display:none;"></div>
</div>

<script>
const token = '${token}'; // Pass token from environment to client-side JS

async function performKvAction(action) {
    const key = document.getElementById('kvKey').value;
    const value = document.getElementById('kvValue').value;
    const messageDiv = document.getElementById('message');
    messageDiv.style.display = 'none';
    messageDiv.className = 'message';

    if (!key && action !== 'list') { // 'list' action not implemented in this example, but for completeness
        showMessage('请填写键。', 'error');
        return;
    }

    let response;
    if (action === 'put') {
        if (!value) {
            showMessage('请填写值。', 'error');
            return;
        }
        const formData = new FormData();
        formData.append('key', key);
        formData.append('value', value);
        formData.append('action', 'put');
        response = await fetch(window.location.pathname + window.location.search, {
            method: 'POST',
            headers: {
                'Authorization': \`Bearer \${token}\`
            },
            body: formData
        });
    } else if (action === 'get') {
        response = await fetch(\`\${window.location.pathname}\${window.location.search}&action=get&key=\${encodeURIComponent(key)}\`, {
            headers: {
                'Authorization': \`Bearer \${token}\`
            }
        });
    } else if (action === 'delete') {
        const formData = new FormData();
        formData.append('key', key);
        formData.append('action', 'delete');
        response = await fetch(window.location.pathname + window.location.search, {
            method: 'POST',
            headers: {
                'Authorization': \`Bearer \${token}\`
            },
            body: formData
        });
    }

    const result = await response.json();
    if (result.success) {
        showMessage(result.message || '操作成功！', 'success');
        if (action === 'get' && result.value !== null) {
            document.getElementById('kvValue').value = result.value;
        } else if (action === 'get' && result.value === null) {
            document.getElementById('kvValue').value = '';
            showMessage('键不存在。', 'error');
        } else if (action === 'delete') {
            document.getElementById('kvValue').value = '';
        }
    } else {
        showMessage(result.message || '操作失败！', 'error');
    }
}

function showMessage(msg, type) {
    const messageDiv = document.getElementById('message');
    messageDiv.innerText = msg;
    messageDiv.className = \`message \${type}\`;
    messageDiv.style.display = 'block';
}
</script>
</body>
</html>
`;
		return new Response(html, {
			headers: { 'Content-Type': 'text/html;charset=utf-8' }
		});
	}

	return new Response('无效的请求', { status: 400 });
}

/**
 * Sends a message to Telegram via Bot API.
 * @param {string} text The message text.
 * @param {string} fromIp The IP address of the sender.
 * @param {string} message The detailed message content.
 * @returns {Promise<void>}
 */
async function sendMessage(text, fromIp, message) {
	if (!BotToken || !ChatID) {
		console.warn('Telegram Bot Token or Chat ID not set. Skipping message sending.');
		return;
	}
	const url = `https://api.telegram.org/bot${BotToken}/sendMessage`;
	const payload = {
		chat_id: ChatID,
		text: `来自 IP: ${fromIp}\n${text}\n${message}`,
		parse_mode: 'HTML'
	};

	try {
		const response = await fetch(url, {
			method: 'POST',
			headers: {
				'Content-Type': 'application/json'
			},
			body: JSON.stringify(payload)
		});
		if (!response.ok) {
			const errorText = await response.text();
			console.error(`Failed to send Telegram message: ${response.status} ${response.statusText} - ${errorText}`);
		}
	} catch (error) {
		console.error('Error sending Telegram message:', error);
	}
}
