import axios from 'axios'
import config from '../../config/config';

class Spark {
    constructor() {

    }

    async getApplication() {
        let response = await axios.get('http://api.bilibili.com/x/web-show/res/loc?pf=0&id=23');
        console.log(response.data);
        return response.data;
    }
}

export default new Spark()