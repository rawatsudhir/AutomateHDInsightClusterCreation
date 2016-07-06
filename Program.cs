using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.Rest;
using Microsoft.Rest.Azure.Authentication;
using Microsoft.Azure;
using Microsoft.Azure.Management.HDInsight;
using Microsoft.Azure.Management.HDInsight.Models;
using Microsoft.Azure.Management.ResourceManager;
using Microsoft.IdentityModel.Clients.ActiveDirectory;
using System.Net.Http;

namespace CreateHDInsightCluster2015Console
{
    class Program
    {
        // The client for managing HDInsight
        private static HDInsightManagementClient _hdiManagementClient;
        private const string ExistingResourceGroupName = "RESOURCE_GROUPE";
        private const string ExistingStorageName = "STORAGE_NAME.blob.core.windows.net";
        private const string ExistingStorageKey = "STORAGE_KEY";
        private const string ExistingBlobContainer = "CONTAINER_NAME";
        private const string NewClusterName = "CLUSTER_NAME";
        private const int NewClusterNumWorkerNodes = 2;
        private const string NewClusterLocation = "CLUSTER_LOCATION";     // Must be the same as the default Storage account
        private const OSType NewClusterOsType = OSType.Windows;
        private const string NewClusterType = "CLUSTER_TYPE"; //HADOOP, SPARK ETC
        private const string NewClusterVersion = "CLUSTER_VERSION";
        private const string NewClusterUsername = "CLUSTER_USER_NAME";
        private const string NewClusterPassword = "CLUSTER_PASSWORD";
        static void Main(string[] args)
        {
            System.Console.WriteLine("Creating a cluster.  The process takes 10 to 20 minutes ...");

            // Authenticate and get a token
            TokenCloudCredentials authToken = GetTokenCloudCredentials();
            // Flag subscription for HDInsight, if it isn't already.
            //EnableHDInsight(authToken);
            // Get an HDInsight management client
                  _hdiManagementClient = new HDInsightManagementClient(authToken);

            // Set parameters for the new cluster
            var parameters = new ClusterCreateParameters
            {
                ClusterSizeInNodes = NewClusterNumWorkerNodes,
                UserName = NewClusterUsername,
                Password = NewClusterPassword,
                Location = NewClusterLocation,
                DefaultStorageAccountName = ExistingStorageName,
                DefaultStorageAccountKey = ExistingStorageKey,
                DefaultStorageContainer = ExistingBlobContainer,
                ClusterType = NewClusterType,
                OSType = NewClusterOsType
            };
            // Create the cluster
            _hdiManagementClient.Clusters.Create(ExistingResourceGroupName, NewClusterName, parameters);

            System.Console.WriteLine("The cluster has been created. Press ENTER to continue ...");
            System.Console.ReadLine();
        }

        public static TokenCloudCredentials GetTokenCloudCredentials()
        {
             //Below infroamtion can be retrieved from Azure Active Directory Application  
            String tenantID = "TENANT_ID";
            String loginEndpoint = "https://login.windows.net/";
            String subscriptionID = "SUBSCRIPTION_ID";
            String authString = loginEndpoint + tenantID;
            String clientID = "CLIENT_ID";
            String key = "KEYS";
            var clientCred = new ClientCredential(clientID, key);
            String resource = "https://management.core.windows.net/";
            AuthenticationContext authenticationContext = new AuthenticationContext(authString, false);
            var authenticationResult = authenticationContext.AcquireToken(resource, clientCred);
            return new TokenCloudCredentials(subscriptionID, authenticationResult.AccessToken); ;
        }
    }
}
