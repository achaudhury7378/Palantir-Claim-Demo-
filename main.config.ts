import { defineConfig } from "@osdk/widget.client";
export default defineConfig({
id: "fraud-network-widget",
name: "Fraud Network Detector",
type: "workshop",
parameters: {
claims: {
type: "objectSet",
objectType: "InsuranceClaim",
displayName: "Filtered Claims"
}
},
events: {
flagSuspicious: {
displayName: "Flag Suspicious Claim",
parameterUpdateIds: ["claims"]
},
investigateCluster: { displayName: "Investigate Cluster" }
}
});
