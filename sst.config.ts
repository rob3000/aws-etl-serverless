import { SSTConfig } from "sst";
import { ETLStack } from './stacks/etl';

export default {
  config(_input) {
    return {
      name: "aws-etl-athena",
      region: "ap-southeast-2",
    };
  },
  stacks(app) {
    app.setDefaultFunctionProps({
      runtime: "go1.x",
    });
    app.stack(ETLStack);
  },
} satisfies SSTConfig;
