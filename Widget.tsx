import React from 'react';
import { useWidgetParameters, useWidgetEvents } from '@osdk/widget.client/react';

export default function FraudNetwork() {
const { claims } = useWidgetParameters<'claims'>();
const { flagSuspicious } = useWidgetEvents<'flagSuspicious'>();

if (!claims?.objects.length) {
return <div>🔍 Filter claims to see fraud networks...</div>;
}

// Simple network viz (upgrade to D3 later)
const highRiskClaims = claims.objects.filter(c => c.properties.anomalyScore > 70);

return (
<div style={{height: '500px', padding: '20px'}}>
<h3>🚨 Fraud Network ({highRiskClaims.length} high-risk claims)</h3>
<div style={{display: 'flex', flexWrap: 'wrap', gap: '10px'}}>
{highRiskClaims.slice(0, 20).map(claim => (
<div key={claim.primaryKey}
style={{padding: '8px', background: '#ff4444', color: 'white', borderRadius: '4px', cursor: 'pointer'}}
onClick={() => flagSuspicious({ claimId: claim.primaryKey })}>
{claim.properties.claimId} (${claim.properties.claimAmount})
</div>
))}
</div>
</div>
);
}
