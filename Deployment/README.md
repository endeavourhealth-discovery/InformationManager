# IM2 Instance Deployment

## AWS Cloudformation

Launch a new instance and enter a name.

Select "My AMIs" then "Owned by me".
Select image `Endeavour-Ubuntu22-Hardened-amd64-2024-05-01_10-32-45`

The instance type should be a minimum of `t2.large`

Select the Key pair `PublicServers`

Edit the Network Settings, select the `Endeavour live` VPC and either the `Public-A1` or `Public-B1` subnet.
Change the Firewall security group to the existing `Admin-Access`.
Add the relevant access group, either `IM2-Restricted` or `IM2-Internet`.

Configure 20GiB storage minimum.

In Advanced Details, set the IAM instance profile to `ServerBuild`

Finally, copy and paste the contents of the `deploy.sh` file into the User data section and launch the instance.
