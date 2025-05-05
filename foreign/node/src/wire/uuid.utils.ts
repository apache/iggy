
import { uuidv7, UUID } from "uuidv7";

export const parse = (uid: string) => UUID.parse(uid);


// https://github.com/LiosK/uuidv7
export const v7 = () => uuidv7();
