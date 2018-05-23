

import Vue from 'vue';
global.Vue = Vue;

global.axios = require('axios');

global.axios.defaults.headers.common = {
    // 'X-CSRF-TOKEN': '',
    'X-Requested-With': 'XMLHttpRequest'
};

const io = require('socket.io-client')
window.socket = io('http://' + nodeHost + ':3000', {
    transports: ['websocket'],
    upgrade: true
});

import PanelVotacion from './components/PanelVotacion';
import Concejal from './components/Concejal';
import ConsultarExpediente from './components/ConsultarExpediente';
import ConsultarSesiones from './components/ConsultarSesiones';
import PanelDisplay from './components/PanelDisplay';
import QuorumButton from './components/QuorumButton';
import VistaBae from './components/VistaBae';
import VistaOd from './components/VistaOd';
import ModalExpediente from './components/ModalExpediente';

Vue.component('panel-votacion', PanelVotacion);
Vue.component('panel-concejal', Concejal);
Vue.component('consultar-expediente', ConsultarExpediente);
Vue.component('consultar-sesiones', ConsultarSesiones);
Vue.component('panel-display', PanelDisplay);
Vue.component('quorum-button', QuorumButton);
Vue.component('vista-bae', VistaBae);
Vue.component('vista-od', VistaOd);
Vue.component('modal-expediente', ModalExpediente);

const moment = require('moment')
require('moment/locale/es')

Vue.use(require('vue-moment'), {
    moment
})

import VueContentPlaceholders from 'vue-content-placeholders'

Vue.use(VueContentPlaceholders)

window.app = new Vue({
    delimiters: ['[[', ']]'],
    el: '#app',
    data: {
        quorum: 0,
        presentes: []
    },
    methods: {},
    mounted: function () {
        socket.on('message', function (message) {
            switch (message.type) {
                case 'quorum':
                    this.quorum = message.data.quorum
                    this.presentes = message.data.presentes
                    break;
            }
        }.bind(this))
    }
});

