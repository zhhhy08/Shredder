namespace Microsoft.WindowsAzure.Governance.ResourcesCache.ShredderService
{
    using Microsoft.AspNetCore.Builder;
    using Microsoft.Extensions.DependencyInjection;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ActivityTracing;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.ServiceConfiguration.Constants;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Enums;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.Shared.Web;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.SharedContracts.Contexts;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.SharedServiceImplementations.Controller.SwaggerGeneration;
    using Microsoft.WindowsAzure.Governance.ResourcesCache.SharedServiceImplementations.Filters;
    using System.Diagnostics.CodeAnalysis;
    using System.Net;
    using System.Reflection;

    [ExcludeFromCodeCoverage]
    public class Startup
    {
        #region Tracing

        private static readonly ActivityMonitorFactory StartupConfigureDependencyResolver
            = new ActivityMonitorFactory("Startup.ConfigureDependencyResolver");

        #endregion

        public Microsoft.Extensions.Configuration.IConfiguration Configuration { get; }

        public Startup(Microsoft.Extensions.Configuration.IConfiguration configuration)
        {
            Configuration = configuration;
        }

        #region Public Methods

        // Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            ConfigureServicesHelper(services);
        }

        public static void ConfigureServicesHelper(IServiceCollection services)
        {
            var thisAssembly = typeof(Startup).GetTypeInfo().Assembly;
            var builder = services.AddMvc(options =>
            {
                options.Filters.Add(new DefaultExceptionFilter());
                options.EnableEndpointRouting = false;
            });
            // Clear parts to not include SharedServiceImplementations controllers
            builder.PartManager.ApplicationParts.Clear();
            builder.AddApplicationPart(thisAssembly)
                   .AddNewtonsoftJson(jsonOptions => JsonTypeFormatter.UpdateApiSerializerSettings(jsonOptions.SerializerSettings));

            if (ServiceConfigConstants.GetEnvironmentType().IsLocal())
            {
                SwaggerConfig.Register(services, PlatformServiceContext.ShredderService.ToString());
            }
        }

        /// <summary>
        /// This code configures Web API. The Startup class is specified as a type
        /// parameter in the WebApp.Start method.
        /// </summary>
        /// <param name="appBuilder">The app builder</param>
        public void Configure(IApplicationBuilder app)
        {
            // Configure the web api App
            ConfigureApp(app, ServiceConfigConstants.GetEnvironmentType());
        }

        #endregion

        #region Private methods

        public static void ConfigureApp(
            IApplicationBuilder app, EnvironmentType environmentType)
        {
            // If running in local mode, disable the SSL verification
            if (environmentType.IsLocal())
            {
                ServicePointManager.ServerCertificateValidationCallback += (sender, certificate, chain, sslPolicyErrors) => true; // lgtm [cs/do-not-disable-cert-validation]
            }

            app.UseMvc();

            if (environmentType.IsLocal())
            {
                app.UseSwagger(options => options.SerializeAsV2 = true);
                // During local build we will generare swagger config for Geneva Actions
            }
        }

        #endregion
    }
}