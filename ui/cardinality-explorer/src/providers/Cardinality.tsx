import { createContext, useContext, useState } from 'react';
import { createCardinalityStore, type CardinalityStoreApi } from '@/stores/useCardinalityStore';

// Context
const CardinalityStoreContext = createContext<CardinalityStoreApi | null>(null);

export const useCardinalityStoreContext = () => {
  const context = useContext(CardinalityStoreContext);
  if (!context) {
    throw new Error('useCardinalityStoreContext must be used within CardinalityProvider');
  }
  return context;
};

// Provider component
export const CardinalityProvider = ({ children }: { children: React.ReactNode }) => {
  const [store] = useState(() => createCardinalityStore());

  return (
    <CardinalityStoreContext.Provider value={store}>
      {children}
    </CardinalityStoreContext.Provider>
  );
};
