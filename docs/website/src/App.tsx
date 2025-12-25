import {
    NavigationItemType,
    PageConstructor,
    PageConstructorProvider,
    Theme,
} from '@gravity-ui/page-constructor';
import {Wrapper} from './components/Wrapper';
import Content from './content.json';
import './styles/overrides.css';

const App = () => {
    return (
        <Wrapper>
            <PageConstructorProvider theme={Theme.Dark}>
                <PageConstructor
                    content={Content}
                    navigation={{
                        header: {
                            withBorder: true,
                            leftItems: [
                                {
                                    arrow: true,
                                    text: 'Documentations',
                                    type: NavigationItemType.Link,
                                    url: 'docs',
                                    urlTitle: 'External website. Opens in a new window',
                                },
                            ],
                            rightItems: [
                                {
                                    text: 'Telegram',
                                    type: NavigationItemType.Link,
                                    url: 'https://t.me/andrei_tserakhau',
                                },
                                {
                                    text: ' Transfer',
                                    theme: 'github',
                                    type: NavigationItemType.Button,
                                    url: 'https://github.com/transferia/transferia',
                                },
                            ],
                        },
                        logo: {
                            icon: 'assets/logo-cropped.svg',
                            text: 'Transfer',
                            urlTitle: 'Transfer',
                        },
                    }}
                />
            </PageConstructorProvider>
        </Wrapper>
    );
};

export default App;
