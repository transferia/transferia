import React from 'react';
import {ThemeProvider} from '@gravity-ui/uikit';

import './Wrapper.scss';

export type AppProps = {
    children: React.ReactNode;
};

export const Wrapper: React.FC<AppProps> = ({children}) => {
    return <ThemeProvider theme={'dark'}>{children}</ThemeProvider>;
};
