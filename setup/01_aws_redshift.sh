#!/bin/bash
# =============================================================================
# AWS Infrastructure for Redshift OpenFlow CDC Demo
# Requires: AWS CLI configured with appropriate permissions
# Region: us-west-2 (adjust as needed)
# =============================================================================

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
if [ -f "$SCRIPT_DIR/../.env" ]; then
    source "$SCRIPT_DIR/../.env"
fi

REGION="${AWS_REGION:-us-west-2}"
NAMESPACE="${REDSHIFT_NAMESPACE:-openflow-demo-ns}"
WORKGROUP="${REDSHIFT_WORKGROUP:-openflow-demo-wg}"
ADMIN_USER="${REDSHIFT_ADMIN_USER:-admin}"
ADMIN_PASS="${REDSHIFT_ADMIN_PASSWORD:-OpenFlowDemo2026}"
DB_NAME="${REDSHIFT_DB:-demo}"

echo "=== Step 1: Create Redshift Serverless Namespace ==="
aws redshift-serverless create-namespace \
    --namespace-name "$NAMESPACE" \
    --admin-username "$ADMIN_USER" \
    --admin-user-password "$ADMIN_PASS" \
    --db-name "$DB_NAME" \
    --region "$REGION"

echo "Waiting for namespace to be available..."
aws redshift-serverless wait namespace-available \
    --namespace-name "$NAMESPACE" \
    --region "$REGION" 2>/dev/null || sleep 30

echo "=== Step 2: Create Redshift Serverless Workgroup ==="
# Get your VPC ID and subnet IDs (adjust for your environment)
VPC_ID=$(aws ec2 describe-vpcs --filters "Name=isDefault,Values=true" \
    --query 'Vpcs[0].VpcId' --output text --region "$REGION")
SUBNET_IDS=$(aws ec2 describe-subnets --filters "Name=vpc-id,Values=$VPC_ID" \
    --query 'Subnets[*].SubnetId' --output text --region "$REGION" | tr '\t' ',')

# For SE sandbox/demo accounts, use private subnets and no public access
aws redshift-serverless create-workgroup \
    --workgroup-name "$WORKGROUP" \
    --namespace-name "$NAMESPACE" \
    --subnet-ids $(echo "$SUBNET_IDS" | tr ',' ' ') \
    --publicly-accessible false \
    --region "$REGION"

echo "Waiting for workgroup to be available..."
while true; do
    STATUS=$(aws redshift-serverless get-workgroup --workgroup-name "$WORKGROUP" \
        --query 'workgroup.status' --output text --region "$REGION" 2>/dev/null)
    echo "  Status: $STATUS"
    [ "$STATUS" = "AVAILABLE" ] && break
    sleep 15
done

echo "=== Step 3: Get Redshift Endpoint IPs ==="
ENDPOINT=$(aws redshift-serverless get-workgroup --workgroup-name "$WORKGROUP" \
    --query 'workgroup.endpoint.address' --output text --region "$REGION")
echo "Redshift endpoint: $ENDPOINT"

# Get the VPC endpoint (Redshift Serverless creates one automatically)
RS_VPC_ENDPOINT=$(aws redshift-serverless get-workgroup --workgroup-name "$WORKGROUP" \
    --query 'workgroup.endpoint.vpcEndpoints[0].vpcEndpointId' --output text --region "$REGION")
echo "Redshift VPC Endpoint: $RS_VPC_ENDPOINT"

# Get the ENI IPs for NLB targets
ENI_IPS=$(aws ec2 describe-network-interfaces \
    --filters "Name=vpc-endpoint-id,Values=$RS_VPC_ENDPOINT" \
    --query 'NetworkInterfaces[*].PrivateIpAddress' --output text --region "$REGION")
echo "ENI IPs for NLB targets: $ENI_IPS"

echo "=== Step 4: Create Network Load Balancer ==="
NLB_ARN=$(aws elbv2 create-load-balancer \
    --name "redshift-demo-nlb" \
    --type network \
    --scheme internal \
    --subnets $(echo "$SUBNET_IDS" | tr ',' ' ') \
    --query 'LoadBalancers[0].LoadBalancerArn' --output text --region "$REGION")
echo "NLB ARN: $NLB_ARN"

NLB_DNS=$(aws elbv2 describe-load-balancers \
    --load-balancer-arns "$NLB_ARN" \
    --query 'LoadBalancers[0].DNSName' --output text --region "$REGION")
echo "NLB DNS: $NLB_DNS"

echo "=== Step 5: Create Target Group and Register Redshift ENIs ==="
TG_ARN=$(aws elbv2 create-target-group \
    --name "redshift-demo-tg" \
    --protocol TCP \
    --port 5439 \
    --vpc-id "$VPC_ID" \
    --target-type ip \
    --health-check-protocol TCP \
    --health-check-port 5439 \
    --query 'TargetGroups[0].TargetGroupArn' --output text --region "$REGION")
echo "Target Group ARN: $TG_ARN"

for IP in $ENI_IPS; do
    aws elbv2 register-targets \
        --target-group-arn "$TG_ARN" \
        --targets "Id=$IP,Port=5439" --region "$REGION"
    echo "  Registered target: $IP:5439"
done

aws elbv2 create-listener \
    --load-balancer-arn "$NLB_ARN" \
    --protocol TCP \
    --port 5439 \
    --default-actions "Type=forward,TargetGroupArn=$TG_ARN" \
    --region "$REGION"
echo "NLB listener created on port 5439"

echo "=== Step 6: Create VPC Endpoint Service ==="
VPCES_ID=$(aws ec2 create-vpc-endpoint-service-configuration \
    --network-load-balancer-arns "$NLB_ARN" \
    --acceptance-required false \
    --query 'ServiceConfiguration.ServiceId' --output text --region "$REGION")
VPCES_NAME=$(aws ec2 describe-vpc-endpoint-service-configurations \
    --service-ids "$VPCES_ID" \
    --query 'ServiceConfigurations[0].ServiceName' --output text --region "$REGION")
echo "VPC Endpoint Service ID: $VPCES_ID"
echo "VPC Endpoint Service Name: $VPCES_NAME"

echo ""
echo "=== OUTPUTS (save these for Snowflake setup) ==="
echo "NLB_DNS=$NLB_DNS"
echo "VPC_ENDPOINT_SERVICE_NAME=$VPCES_NAME"
echo "REDSHIFT_ENDPOINT=$ENDPOINT"
echo ""
echo "Next steps:"
echo "  1. In Snowflake, create a PrivateLink endpoint to $VPCES_NAME"
echo "  2. Run 02_snowflake_networking.sql with NLB_DNS=$NLB_DNS"
echo "  3. Run 03_snowflake_objects.sql"
echo "  4. Run 04_seed_redshift.sql"
