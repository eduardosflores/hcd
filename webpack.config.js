// webpack.config.js
var Encore = require('@symfony/webpack-encore');

var env = require('./env.json');

Encore
// the project directory where all compiled assets will be stored
    .setOutputPath('web/build/')

    // the public path used by the web server to access the previous directory
    // .setPublicPath('/build')
    .setPublicPath(env.publicPath)

    .setManifestKeyPrefix('build')

    // will create web/build/app.js and web/build/app.css
    .addEntry('common-js', './app/Resources/assets/js/common.js')
    .addEntry('app', './app/Resources/assets/js/main.js')
    // .addEntry('admin-lte', './app/Resources/assets/js/adminlte.js')


    .enableVueLoader()

    .addStyleEntry('common', './app/Resources/assets/css/common.scss')
    .addStyleEntry('global', './app/Resources/assets/css/global.scss')
    // .addStyleEntry('admin-lte-style', './app/Resources/assets/css/adminlte.scss')

    // allow sass/scss files to be processed
    .enableSassLoader()


    // allow legacy applications to use $/jQuery as a global variable
    .autoProvidejQuery()

    .autoProvideVariables({
        $: 'jquery',
        jQuery: 'jquery',
        'window.jQuery': 'jquery',
    })

    .configureDefinePlugin((options) => {
        options.nodeHost = JSON.stringify(env.nodeHost);
        // options.baseUrl = JSON.stringify(env.baseUrl);
    })

    .enableSourceMaps(!Encore.isProduction())

    // empty the outputPath dir before each build
    .cleanupOutputBeforeBuild()

    // show OS notifications when builds finish/fail
    .enableBuildNotifications()

// create hashed filenames (e.g. app.abc123.css)
// .enableVersioning()
;

// export the final configuration
module.exports = Encore.getWebpackConfig();
