/**
 * Cloudflare Worker: TeeScanner API Proxy
 * 
 * GitHub Actions (미국 IP) → 이 Worker (Cloudflare 글로벌 에지) → 티스캐너 API (한국)
 * 
 * 배포 방법:
 * 1. https://dash.cloudflare.com 접속 → Workers & Pages → Create
 * 2. "Create Worker" 클릭
 * 3. 이름: teescan-proxy
 * 4. 이 코드를 붙여넣기 → Save and Deploy
 * 5. 생성된 URL (예: https://teescan-proxy.xxxxx.workers.dev)을
 *    GitHub Secrets에 TEESCAN_PROXY_URL로 저장
 */

export default {
  async fetch(request) {
    const corsHeaders = {
      'Access-Control-Allow-Origin': '*',
      'Access-Control-Allow-Methods': 'GET, OPTIONS',
      'Access-Control-Allow-Headers': 'Content-Type',
    };

    // Handle CORS preflight
    if (request.method === 'OPTIONS') {
      return new Response(null, { headers: corsHeaders });
    }

    const url = new URL(request.url);
    const targetUrl = url.searchParams.get('url');

    if (!targetUrl) {
      return new Response(JSON.stringify({ error: 'Missing url parameter' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    // Allow both teescanner.com and golfpang.com domains (security)
    let isTeescan = false;
    let isGolfpang = false;
    try {
      const target = new URL(targetUrl);
      if (target.hostname.endsWith('teescanner.com')) {
        isTeescan = true;
      } else if (target.hostname.endsWith('golfpang.com')) {
        isGolfpang = true;
      } else {
        return new Response(JSON.stringify({ error: 'Domain not allowed. Only teescanner.com and golfpang.com allowed.' }), {
          status: 403,
          headers: { 'Content-Type': 'application/json', ...corsHeaders },
        });
      }
    } catch {
      return new Response(JSON.stringify({ error: 'Invalid URL' }), {
        status: 400,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }

    try {
      const requestMethod = request.method;
      let requestBody = null;
      if (requestMethod === 'POST' || requestMethod === 'PUT') {
        requestBody = await request.text();
      }

      const headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36',
        'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8',
        'Accept-Language': 'ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7',
      };

      const reqContentType = request.headers.get('Content-Type');
      if (reqContentType) {
        headers['Content-Type'] = reqContentType;
      }

      if (isTeescan) {
        headers['Referer'] = 'https://www.teescanner.com/';
        headers['Origin'] = 'https://www.teescanner.com';
      } else if (isGolfpang) {
        headers['Referer'] = 'https://www.golfpang.com/web/round/booking_list.do';
        headers['Origin'] = 'https://www.golfpang.com';
        
        // Forward essential Golfpang headers
        const reqXRequestedWith = request.headers.get('x-requested-with');
        if (reqXRequestedWith) {
          headers['X-Requested-With'] = reqXRequestedWith;
        }
        const reqCustomerCheck = request.headers.get('x-customer-check');
        if (reqCustomerCheck) {
          headers['x-customer-check'] = reqCustomerCheck;
        }
      }

      const response = await fetch(targetUrl, {
        method: requestMethod,
        headers: headers,
        body: requestBody
      });

      const body = await response.text();

      return new Response(body, {
        status: response.status,
        headers: {
          'Content-Type': response.headers.get('Content-Type') || 'application/json',
          ...corsHeaders,
        },
      });
    } catch (err) {
      return new Response(JSON.stringify({ error: `Proxy fetch failed: ${err.message}` }), {
        status: 502,
        headers: { 'Content-Type': 'application/json', ...corsHeaders },
      });
    }
  },
};
