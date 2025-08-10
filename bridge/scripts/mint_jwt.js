const jwt = require('jsonwebtoken');

const secret = process.argv[2];
if (!secret) {
  console.error('Usage: node scripts/mint_jwt.js <JWT_SECRET>');
  process.exit(1);
}
const now = Math.floor(Date.now()/1000);
const token = jwt.sign(
  { iss:'TeevraBridge', aud:'TeevraSheet', iat:now, nbf:now, exp: now + 300 },
  secret,
  { algorithm: 'HS256' }
);
console.log(token);
